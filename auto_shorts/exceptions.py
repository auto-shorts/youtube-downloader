class DifferentBaseLanguagesException(Exception):
    def __init__(self, language_codes: list[str]):
        message = f"Different base language codes - {language_codes}"
        super().__init__(message)
