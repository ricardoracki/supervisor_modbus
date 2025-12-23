from dataclasses import dataclass
from enum import Enum
from src.utils.event_manager import EventManager
from datetime import datetime
import asyncio

class Metrics:
    def __init__(self):
        self.reads_total = 0
        self.reads_success = 0
        self.reads_error = 0
        self.reads_timeout = 0
        self.reconnects_total = 0
        self.last_latency = 0
        self.connected = False
        self.started_at = datetime.now()

    @property
    def uptime(self):
        return (datetime.now() - self.started_at).total_seconds()



class EventTypes(Enum):
    WEIGHT_READ = 'weight_read'
    OPERATION_TYPE_CHANGED = 'operation_type_changed'
    TIMEOUT_ERROR = 'timeout_error'
    ERROR = 'error'


@dataclass
class EventPayload:
    weight: int
    operation_type: int
    classification: int
    reason: int
    state: int
    operation_id: int
    timestamp: datetime



class CheckWeigher(EventManager):
    eventTypes = EventTypes

    def __init__(self, name: str, ip_address: str, port: int, cw_id: str):
        super().__init__()
        self.name = name
        self.ip_address = ip_address
        self.port = port
        self.cw_id = cw_id

        self.metrics = Metrics()
        self.connected = False
        self.enabled = True
        self.timeout = 5.0
        self.poll_interval = 0.1

        self.latency = 0

        self.__last_operation_id = 0    # para controle de transação
        self.__last_operation_type = 0  # para controle de troca de estado
        self._connect_lock = asyncio.Lock()
        
        
    async def read(self):
        """
        Faz a leitura dos dados na rede modbus
        
        """
        print(f"[{self.name}] - Leitura na rede modbus iniciada")
        await asyncio.sleep(2) # simula leitura
        print(f"[{self.name}] - Leitura na rede modbus terminada - Latencia: {self.latency}")
        



        return [1000, 1, 1, 1, 2, 1]

    def dumps(self, data):
        """
        Interpreta os dados lidos 
        
        """
        payload = EventPayload(*data, timestamp = datetime.now() )
        return payload
    
    async def listener(self):
        while self.enabled:
            try:
                start = datetime.now()
                self.metrics.reads_total += 1

                response = await self.safe_read()
                data = self.dumps(response)

                self.metrics.reads_success += 1
                self.metrics.connected = True

                self.latency = (datetime.now() - start).total_seconds()
                self.metrics.last_latency = self.latency

                if data.operation_id != self.__last_operation_id: # Verifica se houve troca de transação
                    # resolve se for pesagem
                    if data.operation_type == 1: 
                        self.dispatch(EventTypes.WEIGHT_READ, data)
                        
                        if self.__last_operation_type == 2:
                            # resolve se for troca de estado para produzindo
                            self.dispatch(EventTypes.OPERATION_TYPE_CHANGED, data) 
                        
                        self.__last_operation_type = 1
                        

                    # resolve se for troca de estado para parado
                    elif data.operation_type == 2 and self.__last_operation_type == 1:
                        self.dispatch(EventTypes.OPERATION_TYPE_CHANGED, data)

                    self.__last_operation_type = data.operation_id
                    


            except asyncio.TimeoutError:
                self.metrics.reads_timeout += 1
                await self.disconnect()
                await self.reconnect_with_backoff()

            except Exception as e:
                self.metrics.reads_error += 1
                self.dispatch(EventTypes.ERROR, e)
                await self.disconnect()
                await self.reconnect_with_backoff()

            await asyncio.sleep(self.poll_interval)

            

    async def connect(self):
        async with self._connect_lock:
            if self.connected:
                return
            print(f"[{self.name}] Conectando...")
            await asyncio.sleep(1)
            self.connected = True
            self.metrics.connected = True


    async def disconnect(self):
        print(f"[{self.name}] Desconectado")
        self.connected = False
        self.metrics.connected = False

    async def safe_read(self):
        if not self.connected:
            await self.connect()

        return await asyncio.wait_for(self.read(), timeout=self.timeout)
    
    async def reconnect_with_backoff(self):
        delay = 1
        max_delay = 30

        while self.enabled and not self.connected:
            try:
                self.metrics.reconnects_total += 1
                await self.connect()
            except Exception:
                print(f"[{self.name}] Falha ao reconectar, retry em {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)


