from enum import Enum, auto

class NetworkEnvironment(Enum):
    INTERNAL = auto()
    EXTERNAL = auto()
    
NET_ENV = NetworkEnvironment.INTERNAL