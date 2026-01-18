from .beauty import Color
from .message import Message


class Developer:
    @staticmethod
    def print_info_of_developer():
        print()
        Message.print_message(
            "Задание было выполнено стажером Big Data в компании Innowise",
            Color.LIGHT_WHITE,
            Color.CYAN,
        )

        Message.print_message(
            "Big Data Engineer: Панфиленко Станислав Игоревич",
            Color.LIGHT_WHITE,
            Color.GREEN,
        )

        Message.print_message(
            "Личная электронная почта: stanislav.panfilenko@gmail.com",
            Color.LIGHT_WHITE,
            Color.BLUE,
        )

        print()


# %%
