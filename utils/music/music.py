import pygame


class BackgroundMusic:
    def __init__(self, file_path: str, loop: bool = True, volume: float = 0.5):
        self.file_path = file_path
        self.loop = loop
        self.volume = volume
        self._initialized = False

    def _init_mixer(self):
        if not self._initialized:
            pygame.mixer.init()
            pygame.mixer.music.set_volume(self.volume)
            self._initialized = True

    def play(self):
        try:
            self._init_mixer()
            pygame.mixer.music.load(self.file_path)
            pygame.mixer.music.play(-1 if self.loop else 0)
        except pygame.error as e:
            print(f"[BackgroundMusic] Ошибка воспроизведения: {e}")

    def stop(self):
        if self._initialized:
            pygame.mixer.music.stop()

    def set_volume(self, volume: float):
        self.volume = max(0.0, min(volume, 1.0))
        if self._initialized:
            pygame.mixer.music.set_volume(self.volume)
