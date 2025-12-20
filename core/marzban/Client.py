import aiohttp
from config import settings as s
from typing import Any
from schemas.schem import CreateUserMarzbanModel
from logger_setup import logger

class MarzbanClient:
    def __init__(
            self,
            base_url: str | str =  s.M_DIGITAL_URL,
            username: str | str =  s.M_DIGITAL_U,
            password: str | str =  s.M_DIGITAL_P,
    ):
        self.base_url: str = base_url
        self.username: str = username
        self.password: str = password

        logger.debug(f"""Base url: {self.base_url} 
Username: {self.username}
Password {self.password[:3]}""")

        self.headers: dict = {
            "accept": "application/json"
        }

        self.inbounds: list = []
        self.template: dict = {
            "username": username,
            "proxies": {
                "vless": {
                    "flow": "xtls-rprx-vision"
                    }
                },
            "inbounds": {},
        }
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        logger.debug('Вошли в контексный менеджер для MarzbanClient')
        return self
    
    async def __aexit__(self, *args):
        await self.session.close()
        logger.debug('Вышли из контекстного менеджера для MarzbanClient')


    async def _token(self):
        data = {
            "username": self.username,
            "password": self.password
        }
        try:
            async with self.session.request(
                method="POST",
                url=f'{self.base_url}/api/admin/token',
                headers=self.headers,
                data=data
            ) as res:
                
                logger.debug(f"Статус от Marzban {res.status}")
                if res.status < 300:
                    res_json = await res.json()
                    token = res_json['access_token']
                    self.headers['Authorization'] = f'Bearer {token}'
                    return res_json
                else:
                    return res.status
        except Exception as e:
            logger.error(e)
            return None


    async def _inbounds(self):
        # Пример ответа
        # {'vless': [{'tag': 'VLESS TCP REALITY', 'protocol': 'vless', 'network': 'tcp', 'tls': 'reality', 'port': 443}]}
        await self._token()

        try:
            async with self.session.request(
                method="GET",
                url=f"{self.base_url}/api/inbounds",
                headers=self.headers
            ) as res:
                
                logger.debug(f"Статус от Marzban {res.status}")
                if res.status == 200:
                    res_json = await res.json()
                    vless = res_json.get("vless", {})
                    
                    for inbound in vless:
                        self.inbounds.append(
                            inbound['tag']
                        )
                    return True
                else:
                    logger.debug(f'Не удалось получить существующие INBOUNDS {res.status}')
        except Exception as e:
            logger.error(e)
            return None


    async def get_users(self) -> dict | None:
        await self._token()

        try:
            async with self.session.request(
                method="GET",
                url=f'{self.base_url}/api/users',
                headers=self.headers
            ) as res:
                
                logger.debug(f"Статус от Marzban {res.status}")
                res_json = await res.json()
                logger.debug(f"Список пользователей: {res_json}")
                return res_json
        except Exception as e:
            logger.error(e)
            return None
    

    async def get_user(self, username: str) -> dict | int | None:
        await self._token()

        try:
            async with self.session.request(
                method="GET",
                url=f'{self.base_url}/api/user/{username}',
                headers=self.headers
            ) as res:
                
                logger.debug(f"Статус от Marzban {res.status}")
                if res.status == 200:    
                    return await res.json()
                else:
                    return res.status
        except Exception as e:
            logger.error(e)
            return None
        
    
    async def create(self, data: CreateUserMarzbanModel):
        await self._inbounds()
        user = self.template
        user['username'] = data.username
        user['inbounds']['vless'] = self.inbounds
        if data.id: user['proxies']['vless']["id"] = data.id
        if data.expire: user["expire"] = data.expire

        try:
            async with self.session.request(
                method="POST",
                url=f'{self.base_url}/api/user',
                headers=self.headers,
                json=user
            ) as res:
                
                logger.debug(f"Статус от Marzban {res.status}")
                if res.status < 210:
                    return await res.json()
                else:
                    return res.status
        except Exception as e:
            logger.error(e)
            return None


    async def delete(self, username: str):
        await self._token()

        try:
            async with self.session.request(
                method="DELETE",
                url=f'{self.base_url}/api/user/{username}',
                headers=self.headers
            ) as res:
                
                logger.debug(f"Статус от Marzban {res.status}")
                if res.status == 200:
                    return await res.json()
                else:
                    return res.status
        except Exception as e:
            logger.error(e)
            return None


    async def modify(self, username: str, expire: int):
        await self._token()
        data = {
            "expire": expire
        }

        try:
            async with self.session.request(
                method="PUT",
                url=f'{self.base_url}/api/user/{username}',
                headers=self.headers,
                json=data
            ) as res:
                
                logger.debug(f"Статус от Marzban {res.status}")
                if res.status == 200:
                    return await res.json()
                else:
                    return res.status
        except Exception as e:
            logger.error(e)
            return None