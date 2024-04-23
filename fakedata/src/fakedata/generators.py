import random
import uuid
from pathlib import Path
from string import ascii_letters, digits

_data_dir = Path(__file__).parent / "data"
with open(_data_dir / "3500常用字.txt", encoding="utf8") as f:
    common_characters = list(f.read())
with open(_data_dir / "1000姓氏.txt", "r", encoding="utf8") as f:
    surname = f.read().splitlines()


def cn_words(length=2):
    return "".join(random.choices(common_characters, k=length))


def cn_name():
    s = random.choice(surname)
    return s + cn_words()


def _unique_phone_factory():
    for x in range(1000_0000, 10000_0000):
        yield f"135{x}"


unique_phone_generator = _unique_phone_factory()


def unique_phone():
    return next(unique_phone_generator)


char_set = digits + ascii_letters


def int_to_base62(n: int):
    rst = []
    while (a := n // 62) >= 0:
        rst.append(char_set[n % 62])
        if a == 0:
            break
        n = a
    rst.reverse()
    return "".join(rst)


def unique_str():
    return int_to_base62(uuid.uuid4().int)
