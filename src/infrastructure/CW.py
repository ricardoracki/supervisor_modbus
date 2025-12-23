import asyncio

from enum import Enum
from datetime import datetime
from dataclasses import dataclass

from pymodbus.client import ModbusTcpClient

from src.core.logger import get_logger
from src.utils.event_manager import EventManager
from src.core.types.ModbusReadPayload import ModbusReadPayload


GAP_ADDRESS = 30720
SIZE_READ = 11
logger = get_logger(__name__)


@dataclass
class Metrics:

    def __init__(self):
        self.reads_total = 0
        self.reads_success = 0
        self.reads_error = 0
        self.reads_timeout = 0
        self.reconnects_total = 0
        self.last_latency: float = 0
        self.latency: float = 0
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


class CheckWeigher(EventManager):
    eventTypes = EventTypes

    def __init__(self, name: str, ip_address: str, port: int, cw_id: str, **kwargs):
        super().__init__()
        self.name = name
        self.ip_address = ip_address
        self.port = port
        self.cw_id = cw_id

        self.enabled = kwargs.get('enabled', True)
        self.timeout = kwargs.get('timeout', 5.0)
        self.poll_interval = kwargs.get('poll_interval', 0.1)

        self.metrics = Metrics()
        self.connected = False
        self.registers: None | ModbusReadPayload = None

        self.__modbusClient = ModbusTcpClient(ip_address, port=port)

        self.__last_operation_id = 0    # para controle de transação
        self.__last_operation_type = 0  # para controle de troca de estado
        self._connect_lock = asyncio.Lock()

    async def read(self) -> list[int]:
        """
        Faz a leitura dos dados na rede modbus

        """
        logger.debug(f"[{self.name}] - Leitura na rede modbus iniciada")
        response = self.__modbusClient.read_holding_registers(
            address=GAP_ADDRESS, count=SIZE_READ)
        logger.debug(
            f"[{self.name}] - Leitura na rede modbus terminada - Latencia: {self.metrics.latency}")

        return response.registers

    def dumps(self, data) -> ModbusReadPayload:
        """
        Interpreta os dados lidos 

        """

        self.payload = ModbusReadPayload(
            cw_id=self.cw_id,
            operation_type=data[0],
            weight=data[1],
            classification=data[2],
            ppm=data[3],
            reason=data[7],
            operation_id=data[10],
            timestamp=datetime.now()
        )

        return self.payload

    async def listener(self):
        while self.enabled:
            try:
                start = datetime.now()
                self.metrics.reads_total += 1

                response = await self.safe_read()
                data = self.dumps(response)

                self.metrics.reads_success += 1
                self.metrics.connected = True

                self.metrics.latency = (datetime.now() - start).total_seconds()
                self.metrics.last_latency = self.metrics.latency

                if data.operation_id != self.__last_operation_id:  # Verifica se houve troca de transação
                    if data.operation_type == 1:
                        # resolve se for pesagem
                        await self.dispatch(EventTypes.WEIGHT_READ, data)

                        if self.__last_operation_type == 2:
                            # resolve se for troca de estado para produzindo
                            await self.dispatch(
                                EventTypes.OPERATION_TYPE_CHANGED, data)

                        self.__last_operation_type = 1

                    # resolve se for troca de estado para parado
                    elif data.operation_type == 2 and self.__last_operation_type != 2:
                        await self.dispatch(EventTypes.OPERATION_TYPE_CHANGED, data)

                    # Para controle de estado produzindo ou parado
                    self.__last_operation_type = data.operation_type
                    # Para controle de transação (Evita processar duas vezes a mesma transação)
                    self.__last_operation_id = data.operation_id

            except asyncio.TimeoutError:
                self.metrics.reads_timeout += 1
                await self.disconnect()
                await self.reconnect_with_backoff()

            except Exception as e:
                self.metrics.reads_error += 1
                await self.dispatch(EventTypes.ERROR, e)
                await self.disconnect()
                await self.reconnect_with_backoff()

            await asyncio.sleep(self.poll_interval)

    async def connect(self):
        async with self._connect_lock:
            if self.connected:
                return
            logger.info(f"[{self.name}] Conectando...")

            if self.__modbusClient.connect():
                self.connected = True
                self.metrics.connected = True
                logger.info(f"[{self.name}] conectado")
                return True

    async def disconnect(self):
        logger.info(f"[{self.name}] Desconectado")
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
            except Exception as e:
                logger.exception(
                    f"[{self.name}] Falha ao reconectar, retry em {delay}s", e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)
