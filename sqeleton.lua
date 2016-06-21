-- Welcome to sqeleton.lua! Here's how it works technically:
--
-- This a a work queue. The work queue is divided into channels. Think of a
-- channel as a separate workspace for different types of jobs. You can use as
-- many or as few channels as you like. When consumers as for jobs, they ask for
-- them in a specific channel.
--
-- Each job added to the queue is given a unique (queue-unique, not channel-
-- unique) auto-incremented integer ID. This is is used to track the job across
-- calls and also used to give priority (all things equal, lower IDs take
-- precedence because they were created first).
--
-- Jobs also have a unique key, "sqeleon:job:[job_id]", which stores both the
-- jobs payload and statistics (reserves, buries, etc).
--
-- Each channel is comprised of a few sections:
--   * a ready bucket. jobs go here when they are up for grabs for a consumer.
--     this is a sorted set, ordered by the job priority (ASC) and job id (ASC).
--   * a pending bucket. jobs go here when they are being worked on. this is a
--     sorted set, ordered by the current timestamp (in ms, passed by the
--     producer) + the job's time to run ("ttr") value. This means that jobs
--     that timeout first will be given priority.
--   * a buried bucket. jobs go here if they are buried, which means they are
--     kept in the system but not available for processing (until kicked). this
--     is good for failed jobs that require human inspection.
--   * a stats bucket. it tracks job reserves, deleted, enqueues, dequeues, etc.
--
-- All queue operations having to do with ready/pending state operate only on
-- the job's ID value, never the job payload itself.
--
-- When a job is enqueued, it is placed in the ready bucket if it has no delay,
-- or the pending bucket if there is a delay.
--
-- When a job is dequeued, it is removed from the ready bucket and placed into
-- the pending bucket. It remains there until it is deleted or released. If a
-- job times out (ttr) while in the pending bucket, it remains there until the
-- next dequeue operation comes through, at which point the job is returned and
-- its ttr is reset (and it's kept in pending).
--
-- Once a job is completed, it is either released (meaning "I am unable to do
-- this task, someone else should do it) back into the ready state, or it is
-- deleted from the queue system if completed successfully. A job can also be
-- buried, meaning it will not be grabbed by any consumers until "kicked." This
-- is good for faiures that require human inspection.

-- used to treat arrays like hashes
local tohash = function(array)
	local num = table.getn(array)
	local i = 1
	local myhash = {}
	while i <= num do
		local key = array[i]
		local val = array[i+1]
		myhash[key] = val
		i = i + 2
	end
	return myhash
end

-- utility function to mark a job as "ready"
local do_make_ready = function(channel, job_id, priority)
	local chankey = 'sqeleton:channel:'..channel
	local sortval = priority + (job_id / math.pow(10, 11))
	redis.call('zadd', chankey..':ready', sortval, job_id)
end

-- utility function to mark a job as "reserved"
local do_reserve = function(channel, job_id, timestamp, ttr, is_delay)
	local chankey = 'sqeleton:channel:'..channel
	local sortval = timestamp + (ttr * 1000)
	redis.call('zrem', chankey..':ready', job_id)
	redis.call('zadd', chankey..':pending', sortval, job_id)
	if not is_delay then
		redis.call('hincrby', 'sqeleton:job:'..job_id, 'reserves', 1);
		redis.call('hincrby', 'sqeleton', 'stats:reserves', 1);
		redis.call('hincrby', chankey, 'stats:reserves', 1);
	end
end

-- enqueue a new job
local enqueue = function(channel, data, ttr, priority, delay, timestamp)
	-- get the next job id
	local job_id = tostring(redis.call('hincrby', 'sqeleton', 'next-id', 1))

	-- store the job data locally
	redis.call('hmset', 'sqeleton:job:'..job_id,
		'id', job_id,
		'channel', channel,
		'data', data,
		'ttr', ttr,
		'priority', priority
	)

	-- add the job to the given channel
	if delay > 0 then
		-- if we have a delay, pretend the job is running
		do_reserve(channel, job_id, timestamp, delay, true)
	else
		-- if there's no delay, put the job in the ready queue
		do_make_ready(channel, job_id, priority)
	end

	local chankey = 'sqeleton:channel:'..channel
	redis.call('hincrby', 'sqeleton', 'stats:enqueued', 1)
	redis.call('hincrby', chankey, 'stats:enqueued', 1)

	-- return the job id
	return job_id
end

-- grab the next job off the list. first checks the reserved set for expired
-- jobs. if one is found, its timeout is reset and it's returned. if no expired
-- job is found, we pull the next one off the ready list.
local dequeue = function(channel, timestamp)
	local chankey = 'sqeleton:channel:'..channel

	-- first check the "pending" queue for expired jobs
	local job = redis.call('zrange', chankey..':pending', 0, 0, 'withscores');

	-- check if the job is expired. if so, keep it
	if job[1] then
		local job_id = job[1]
		local job_expire = tonumber(job[2])
		if job_expire <= timestamp then
			redis.call('hincrby', 'sqeleton:job:'..job_id, 'timeouts', 1)
			local jobarr = redis.call('hgetall', 'sqeleton:job:'..job_id)
			local job = tohash(jobarr)
			local ttr = job['ttr']
			local priority = job['priority']
			do_reserve(channel, job_id, timestamp, ttr, false)
			-- tally/return our reserve count
			local reserves = job['reserves']
			if not reserves then reserves = 0 end
			table.insert(jobarr, 'reserves')
			table.insert(jobarr, tostring(reserves + 1))
			return jobarr
		end
	end

	-- no expired jobs found, grab the next off the ready queue
	job = redis.call('zrange', chankey..':ready', 0, 0)
	local job_id = job[1]
	if job_id then
		local jobarr = redis.call('hgetall', 'sqeleton:job:'..job_id)
		local job = tohash(jobarr)
		local ttr = job['ttr']
		do_reserve(channel, job_id, timestamp + ttr, ttr, false)
		-- tally/return our reserve count
		local reserves = job['reserves']
		if not reserves then reserves = 0 end
		table.insert(jobarr, 'reserves')
		table.insert(jobarr, tostring(reserves + 1))
		return jobarr
	end
	return nil
end

-- deleted a job (complete it)
local delete = function(job_id)
	local channel = redis.call('hget', 'sqeleton:job:'..job_id, 'channel')
	if not channel then
		return nil
	end

	local chankey = 'sqeleton:channel:'..channel
	redis.call('zrem', chankey..':pending', job_id)
	redis.call('zrem', chankey..':ready', job_id)
	redis.call('del', 'sqeleton:job:'..job_id)
	redis.call('del', 'sqeleton:buried:'..job_id)
	redis.call('hincrby', 'sqeleton', 'stats:deletes', 1)
	redis.call('hincrby', chankey, 'stats:deletes', 1)
	return 1
end

-- release a job (let someone else handle it)
local release = function(job_id, delay, priority, timestamp)
	local job = tohash(redis.call('hgetall', 'sqeleton:job:'..job_id))
	if not job['id'] then
		return nil
	end

	local channel = job['channel']
	local ttr = job['ttr']
	if not priority or priority < 0 then
		priority = job['priority']
	else
		redis.call('hset', 'sqeleton:job:'..job_id, 'priority', priority)
	end
	local chankey = 'sqeleton:channel:'..channel
	local exists = redis.call('zrem', chankey..':pending', job_id)
	if exists == 0 then
		return nil
	end

	if delay and delay > 0 then
		do_reserve(channel, job_id, timestamp, ttr, true)
	else
		do_make_ready(channel, job_id, priority)
	end
	redis.call('hincrby', 'sqeleton:job:'..job_id, 'releases', 1);
	redis.call('hincrby', 'sqeleton', 'stats:releases', 1);
	redis.call('hincrby', chankey, 'stats:releases', 1);
	return 1
end

-- bury a job. generally you do this when a job has failed a number of times and
-- you want to stop it from running but you want it there for further inspection
-- later.
local bury = function(job_id, errmsg)
	local channel = redis.call('hget', 'sqeleton:job:'..job_id, 'channel')
	if not channel then
		return nil
	end

	local chankey = 'sqeleton:channel:'..channel
	-- remove the job from pending/running queues and "bury" it
	redis.call('zrem', chankey..':pending', job_id)
	redis.call('zrem', chankey..':ready', job_id)
	redis.call('hmset', 'sqeleton:buried:'..job_id,
		'errmsg', errmsg
	)
	redis.call('hincrby', 'sqeleton:job:'..job_id, 'buries', 1);
	redis.call('hincrby', 'sqeleton', 'stats:buries', 1);
	redis.call('hincrby', chankey, 'stats:buries', 1);
	return 1
end

-- kick a job (essentially means moving it from the "buried" state to "ready")
local kick = function(job_id, priority)
	local channel = redis.call('hget', 'sqeleton:job:'..job_id, 'channel')
	if not channel then
		return nil
	end

	local chankey = 'sqeleton:channel:'..channel
	local exists = redis.call('del', 'sqeleton:buried:'..job_id)
	if exists == 0 then
		return nil
	end
	do_make_ready(channel, job_id, priority)
	redis.call('hincrby', 'sqeleton:job:'..job_id, 'kicks', 1);
	redis.call('hincrby', 'sqeleton', 'stats:kicks', 1);
	redis.call('hincrby', chankey, 'stats:kicks', 1);
	return 1
end

-- wipe the entire queue clean. remove all jobs, stats, tracking, channels, etc
-- from the queue. this is an expensive operation, so use wisely.
local wipe = function()
	-- create at least one sqeleton:* entry for our query below
	redis.call('set', 'sqeleton:deleteme', 1);
	local keys = redis.call('keys', 'sqeleton:*') 
	for i=1,#keys,1000 do 
		redis.call('del', unpack(keys, i, math.min(i+999, #keys)));
	end 
	--redis.call('del', unpack(redis.call('keys', 'sqeleton:*')))
	redis.call('del', 'sqeleton')
	return 1
end

-- dispatch section
local cmd = ARGV[1]
if cmd == "enqueue" then
	local channel = ARGV[2]
	local data = ARGV[3]
	local ttr = tonumber(ARGV[4])
	local priority = tonumber(ARGV[5])
	local delay = tonumber(ARGV[6])
	local timestamp = tonumber(ARGV[7])
	return enqueue(channel, data, ttr, priority, delay, timestamp)
elseif cmd == "dequeue" then
	local channel = ARGV[2]
	local timestamp = tonumber(ARGV[3])
	return dequeue(channel, timestamp)
elseif cmd == "delete" then
	local job_id = ARGV[2]
	return delete(job_id)
elseif cmd == "release" then
	local job_id = ARGV[2]
	local delay = tonumber(ARGV[3])
	local priority = tonumber(ARGV[4])
	local timestamp = tonumber(ARGV[5])
	return release(job_id, delay, priority, timestamp)
elseif cmd == "bury" then
	local job_id = ARGV[2]
	local errmsg = ARGV[3]
	return bury(job_id, errmsg)
elseif cmd == "kick" then
	local job_id = ARGV[2]
	local priority = tonumber(ARGV[3])
	return kick(job_id, priority)
elseif cmd == "wipe" then
	return wipe()
end

