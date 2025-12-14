import pytest
from datetime import datetime
from core.marzban.Client import MarzbanClient
from schemas.schem import CreateUserMarzbanModel


@pytest.mark.asyncio
async def test_get_token():
    async with MarzbanClient() as client:
        res = await client._token()

    assert type(res) == dict


@pytest.mark.asyncio
async def test_get_token_wrong_data():
    class MarzbanWrongData(MarzbanClient):
        def __init__(self):
            super().__init__(username="wrong", password="wrong")
    
    async with MarzbanWrongData() as client:
        res = await client._token()
    
    assert res == 401


@pytest.mark.asyncio
async def test_create_user():
    data = CreateUserMarzbanModel(
        username="username_for_tests"
    )

    async with MarzbanClient() as client:
        res = await client.create(data=data)

    assert type(res) == dict


@pytest.mark.asyncio
async def test_modify_user():
    date = datetime.timestamp(datetime.now())

    async with MarzbanClient() as client:
        res = await client.modify(username="username_for_tests",expire=int(date))

    assert type(res) == dict


@pytest.mark.asyncio
async def test_get_user():
    async with MarzbanClient() as client:
        res = await client.get_user(username="username_for_tests")

    assert type(res) == dict


@pytest.mark.asyncio
async def test_delete_user():
    async with MarzbanClient() as client:
        res = await client.delete(username='username_for_tests')

    assert type(res) == dict