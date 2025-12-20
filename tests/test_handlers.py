from tests.fixtures import message, callback_query, create_user, create_user_in_links
import pytest, pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession
from db.models import Base, User, UserLinks
from repositories.base import BaseRepository
from datetime import datetime, timedelta
from config import settings
from redis.asyncio import Redis
import asyncio, json
from misc.utils import pub_listner
from core.yoomoney.payment import YooPay


@pytest_asyncio.fixture
async def redis_client():
    """Один Redis клиент на всю сессию"""
    client = Redis(
        host='localhost',
        port=6379,
        decode_responses=True
    )
    yield client
    await client.flushall()
    await client.aclose()


@pytest_asyncio.fixture(autouse=True)
async def clear_redis(redis_client):
    """Очищаем Redis перед каждым тестом"""
    await redis_client.flushdb()
    yield


@pytest_asyncio.fixture(scope="session")
async def test_engine():
    """Движок для тестовой БД"""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False
    )
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def test_session_maker(test_engine):
    """Session maker для тестов"""
    return async_sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )


@pytest_asyncio.fixture
async def test_session(test_session_maker):
    """Одна сессия для теста"""
    async with test_session_maker() as session:
        yield session


@pytest_asyncio.fixture
async def running_listener():
    redis_cli = Redis(decode_responses=True)
    task = asyncio.create_task(pub_listner(redis_cli=redis_cli))
    await asyncio.sleep(0.1)  # Даём время на запуск
    print("Listner запущен")
    yield task
    task.cancel()
    try:
        await task
        await redis_cli.flushall()
    except asyncio.CancelledError:
        pass


def get_keyboard_buttons(mock_call):
    """Получить все кнопки из reply_markup"""
    if 'reply_markup' in mock_call.kwargs:
        keyboard = mock_call.kwargs['reply_markup']
    else:
        keyboard = mock_call.args[1] if len(mock_call.args) > 1 else None
    
    if not keyboard:
        return []
    
    # Flatten все кнопки
    buttons = []
    for row in keyboard.inline_keyboard:
        buttons.extend(row)
    
    return buttons


@pytest.mark.asyncio
async def test_start_command_says_hello(message, test_session: AsyncSession, redis_client):
    """Тест: /start отвечает 'Привет'"""
    from handlers.start import start_command

    msg = message()

    await start_command(message=msg, session=test_session, redis_cache=redis_client)
    msg.answer.assert_called_once()

    repo = BaseRepository(session=test_session, model=User)
    res = await repo.get_one(user_id=msg.from_user.id)
    
    # Проверяем только текст (игнорируем reply_markup)
    call_kwargs = msg.answer.call_args.kwargs
    assert call_kwargs['text'] == 'Привет!'
    assert 'Привет' in call_kwargs['text']

    assert res is not None
    assert res.user_id == msg.from_user.id


@pytest.mark.asyncio
async def test_start_new_user(message, test_session: AsyncSession, redis_client):
    """Тест: выбор тарифа показывает 4 вариантаб если пользователь новый"""
    from handlers.start import start_command
    msg = message()

    await start_command(message=msg, session=test_session, redis_cache=redis_client)
    buttons = get_keyboard_buttons(msg.answer.call_args)
    
    assert len(buttons) == 4
    
    # Проверяем тексты
    button_texts = [btn.text for btn in buttons]
    assert '🎁 Пробный период' in button_texts
    assert '💳 Оплатить' in button_texts
    assert '🔗 Подписка и ссылки' in button_texts
    assert '📱 Инструкция' in button_texts


@pytest.mark.asyncio
async def test_start_user_used_trial(message, test_session: AsyncSession, create_user, redis_client):
    """
    Тест: Если у пользователя используется пробный период,
    то он не отображается
    """
    from handlers.start import start_command
    user_id = 1234
    username = 'trial_used'
    trial_used = True
    subscription_end = datetime.now() - timedelta(days=1)

    await create_user(
        user_id=user_id,
        username=username,
        trial_used=trial_used,
        subscription_end=subscription_end,
    )
    repo = BaseRepository(session=test_session, model=User)
    user = await repo.get_one(user_id=user_id)

    assert user is not None
    assert user.user_id == user_id
    assert user.trial_used is True

    msg = message(
        user_id=user_id, 
        username=username
    )

    await start_command(message=msg, session=test_session, redis_cache=redis_client)

    buttons = get_keyboard_buttons(msg.answer.call_args)
    
    assert len(buttons) == 3
    
    # Проверяем тексты
    button_texts = [btn.text for btn in buttons]
    assert '💳 Оплатить' in button_texts
    assert '🔗 Подписка и ссылки' in button_texts
    assert '📱 Инструкция' in button_texts


@pytest.mark.asyncio
async def test_if_pay_have_keys(callback_query, running_listener, redis_client):
    """
    Тест: отображаеются ли кнопки оплаты при нажатии
    """
    from handlers.payment import choose_sum

    clb = callback_query()
    
    await choose_sum(clb, redis_cache=redis_client)

    buttons = get_keyboard_buttons(clb.message.edit_text.call_args)

    assert len(buttons) == 5

    button_texts = [btn.text for btn in buttons]
    button_clb = [btn.callback_data for btn in buttons]
    
    prices = [
        ("📅 50 ₽ • 1 месяц", 'pay_50'),
        ("📆 150 ₽ • 3 месяца", 'pay_150'),
        ("🗓 300 ₽ • 6 месяцев", 'pay_300'),
        ("📋 600 ₽ • 12 месяцев", 'pay_600')
    ]
    await asyncio.sleep(0.5)

    for text, clb_but in prices:
        assert text in button_texts 
        assert clb_but in button_clb
    assert '⬅️ Назад' in button_texts
    assert 'start_menu' in button_clb


@pytest.mark.asyncio
async def test_if_instriction_have_keys(
    callback_query, 
    test_session: AsyncSession, 
    create_user,  # ← Добавили
    create_user_in_links, 
    redis_client
):
    """Тест: отображаются ли кнопки инструкции при нажатии"""
    from handlers.instructions import menu
    import uuid

    uuid_user = str(uuid.uuid4())
    user_id = 111111

    clb = callback_query(user_id=user_id)

    # ✅ Сначала создаём пользователя с подпиской
    await create_user(
        user_id=user_id,
        username="instruser",
        subscription_end=datetime.now() + timedelta(days=30)  # ← Активная подписка!
    )

    # ✅ Потом создаём ссылки
    await create_user_in_links(
        user_id=user_id,
        uuid=uuid_user,
        panel1='panel1'
    )

    await menu(clb, test_session, redis_cache=redis_client)

    # ✅ Теперь edit_text должен быть вызван
    clb.message.edit_text.assert_called_once()
    
    buttons = get_keyboard_buttons(clb.message.edit_text.call_args)
    
    repo = BaseRepository(session=test_session, model=UserLinks)
    user = await repo.get_one(user_id=user_id)

    assert user is not None
    assert user.user_id == user_id
    assert user.uuid == uuid_user

    assert len(buttons) == 2

    button_texts = [btn.text for btn in buttons]
    button_clb = [btn.callback_data for btn in buttons]
    button_web = [btn.web_app for btn in buttons if btn.web_app is not None]
    
    # ✅ Безопасно получаем web_app
    web_app = button_web[0] if button_web else None
    assert web_app is not None, "WebApp кнопка не найдена"

    instruction = [('📱 Инструкция по установке', f"{settings.IN_SUB_LINK}1234")]

    for text, clb_url in instruction:
        assert text in button_texts
        assert clb_url in web_app.url
    
    assert '⬅️ Назад' in button_texts
    assert 'start_menu' in button_clb


@pytest.mark.asyncio
async def test_if_instriction_answers_new_user(callback_query, test_session: AsyncSession, redis_client):
    """
    Тест: отображаеются ли кнопки инстркукции при нажатии
    """
    from handlers.instructions import menu

    clb = callback_query()

    await menu(clb, test_session, redis_client)

    clb.answer.assert_called_once()

    answer_text = clb.answer.call_args.kwargs['text']
    assert answer_text == 'У вас нет подписки'


@pytest.mark.asyncio
async def test_if_payments_creates_link(callback_query, test_session: AsyncSession, redis_client, running_listener):
    """
    Тест: отображаеются ли кнопки при выборе суммы оплаты
    """
    from handlers.payment import payment_process

    payment_clb = ('pay_50', "pay_150", "pay_300", "pay_600")
    yoo_handl = YooPay()
    amount = 50

    res = await yoo_handl.create_payment(amount=amount, email="saivel.mezencev1@gmail.com", plan="1+9210")
    pay_str = "POP_PAY_CHOOSE:123"

    data_for_load = {
        "payment_url": res[0], #type: ignore 
        "payment_id": res[1] #type: ignore 
    }

    await redis_client.set(pay_str, json.dumps(data_for_load), ex=600)

    for callb in payment_clb:
        clb = callback_query(
            data=callb
        )
        
        await payment_process(clb, redis_cache=redis_client)
        
        buttons = get_keyboard_buttons(clb.message.edit_text.call_args)
        answer_text = clb.message.edit_text.call_args[1]['text']

        val = callb.split("_")[1]
        txt = f"Сумма для оплаты {val}"
        buttons_texts_should = ['💳 Перейти к оплате', '⬅️ Назад']

        button_texts = [btn.text for btn in buttons]
        assert len(buttons) == 2

        assert buttons_texts_should[0] in button_texts
        assert buttons_texts_should[1] in button_texts