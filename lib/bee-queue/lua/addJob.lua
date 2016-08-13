--[[
key 1 -> bq:name:id (job ID counter)
key 2 -> bq:name:jobs
key 3 -> bq:name:waiting
key 4 -> job id
key 5 -> job now
arg 1 -> job data
]]

local jobId = KEYS[4]
local now = KEYS[5]

if (jobId=="false") then
  jobId = redis.call("incr", KEYS[1])
end


redis.call("hset", KEYS[2], jobId, ARGV[1])

--if immediate...
if (now=="false") then
  redis.call("lpush", KEYS[3], jobId)
else
  redis.call("rpush", KEYS[3], jobId)
end




return jobId
