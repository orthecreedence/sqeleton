Sqeleton
--------

A simple work queue for redis written in Lua. The API closely follows [beanstalkd](http://kr.github.io/beanstalkd/).

The purpose of this library is to provide a *generic* queuing system with only
one hard dependency: Redis >= 2.6. There's no service required to run in Node,
Ruby, Python, QBasic, etc.

If you can use beanstalkd, then use it. It's great and works perfectly for what
it was created for. However, if you already have redis laying around in your
system, why not put it to work?

## Setup

First things first: to run the queue

