import numpy as np

from .beauty import Color


class Message:
    @staticmethod
    def print_message(line, cover_color, text_color):
        print()
        lines = f"{Color.BOLD}{cover_color}+{'-' * (len(line) + 2)}+{Color.RESET}"
        print(lines)
        print(
            f"{Color.BOLD}{cover_color}| {text_color}{Color.BOLD}{line}{Color.RESET}{cover_color} |{Color.RESET}"
        )
        print(lines)

    @staticmethod
    def print_matrix(array: np.ndarray, message: str):
        Message.print_message(message, Color.BLUE, Color.LIGHT_WHITE)
        rows, columns = array.shape
        for t in range(rows):
            print(Color.BOLD + Color.PURPLE + "(  ", end="")
            for k in range(columns):
                val = str(array[t, k]).ljust(3)
                print(Color.BOLD + Color.LIGHT_WHITE + val, end="")
            print(Color.BOLD + Color.PURPLE + ")\n", end="")

    @staticmethod
    def wait_for_enter():
        Message.print_message(
            "Нажмите Enter, чтобы продолжить...", Color.BLUE, Color.CYAN
        )
        input()
