from granian import Granian
from granian.constants import Loops, Interfaces
from setup_webhook import setup_webhook
import asyncio


if __name__ == "__main__":
    try:
        asyncio.run(setup_webhook())
        Granian(
            target="app.main:app",
            address="127.0.0.1",
            port=8000,
            workers=2,
            loop=Loops.asyncio,
            log_enabled=True,
            interface=Interfaces.ASGI,
            reload=True
        ).serve()
    except Exception as e:
        print(e)
