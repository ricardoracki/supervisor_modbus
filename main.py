from time import sleep
from src.core.CW import CheckWeigher, EventTypes
import asyncio

cw1 = CheckWeigher(name='CW1', ip_address='127.0.0.1', port=1, cw_id='1')
cw2 = CheckWeigher(name='CW2', ip_address='127.0.0.1', port=5, cw_id='2')

cw1.on(EventTypes.WEIGHT_READ, print)
cw1.on(EventTypes.OPERATION_TYPE_CHANGED, print)

cw2.on(EventTypes.WEIGHT_READ, print)
cw2.on(EventTypes.OPERATION_TYPE_CHANGED, print)

cw2.on(EventTypes.TIMEOUT_ERROR, lambda *_: print('TIMEOUT_ERROR'))

cws = [cw1, cw2]

async def main():
    tasks = [asyncio.create_task(cw.listener()) for cw in cws]
    await asyncio.gather(*tasks)

asyncio.run(main())