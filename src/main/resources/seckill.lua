-- 优惠券id
local voucherId = ARGV[1]
-- 用户id
local userId = ARGV[2]
-- 订单id
local orderId = ARGV[3]

-- 库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 订单key
local orderKey = 'seckill:order:' .. voucherId

-- 判断库存是否充足
if (tonumber(redis.call('get', stockKey)) <= 0) then
    -- 库存不足
    return 1
end

-- 判断是否重复抢购
if (redis.call('sismember', orderKey, userId) == 1) then
    -- 重复抢购
    return 2
end

--扣除库存
redis.call('incrby', stockKey, -1)
--下单 保存用户
redis.call('sadd', orderKey, userId)

--将订单放入消息队列中 xadd stream.orders * k1 v1 k2 v2 ....
redis.call('xadd', 'stream.orders', '*', 'voucherId', voucherId, 'userId', userId, 'id', orderId)

return 0