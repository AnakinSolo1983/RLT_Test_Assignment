from aiogram import Bot, Dispatcher, types
import asyncio
#from os import getenv, environ
import telebot
from main import aggregate_data
from aiogram.filters import CommandStart
import json

#-------

# Setting up the Telegram bot using a provided token.
BOT_TOKEN = "7380601795:AAFLzkUW35xXpsVlVD3Rvg5hXmTZ_88jZ_0"
bot = Bot(BOT_TOKEN)
dispatch = Dispatcher()

#-------

# Function for the '/start' command.
@dispatch.message(CommandStart())
async def start(message: types.Message):
    
    user_name = message.from_user.first_name
    await message.answer(f"Добро пожаловать, {user_name}. Этот бот принимает от пользователей текстовые сообщения содержащие JSON с входными данными для агрегации статистических данных о зарплатах сотрудников компании по временным промежуткам, и возвращает агрегированные данные в ответ.")


@dispatch.message()
async def get_message(input: types.Message):

    inputs = json.loads(input.text) # Extracting JSON data from incoming messages, extracting parameters, aggregating data, and sending back the results.
    dt_from = inputs['dt_from']
    dt_upto = inputs['dt_upto']
    group_type = inputs['group_type']

    answer = await aggregate_data(dt_from,dt_upto,group_type)
    await input.answer(str(answer).replace("'", '"'))

# Starting the bot and handling message polling asynchronously.
async def main():
    
    await dispatch.start_polling(bot)

if __name__ == "__main__":
    
    asyncio.run(main())
