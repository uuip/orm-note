import unittest

from argon2 import PasswordHasher
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, select, text

from sa.types.password import PassWord, make_password, verify_password


class PasswordTests(unittest.TestCase):
    def test_hash_and_verify(self):
        for password in ("secret", "密码-🔑", "", "a" * 1000):
            with self.subTest(password=password):
                hashed = make_password(password)
                self.assertTrue(hashed.startswith("$argon2id$"))
                self.assertTrue(PasswordHasher().verify(hashed, password))
                self.assertTrue(verify_password(password, hashed))
                self.assertFalse(verify_password(password + "wrong", hashed))

    def test_verify_existing_argon2_hash(self):
        hashed = PasswordHasher().hash("existing-password")
        self.assertTrue(verify_password("existing-password", hashed))
        self.assertFalse(verify_password("wrong", hashed))

    def test_hash_uses_random_salt(self):
        first = make_password("secret")
        second = make_password("secret")
        self.assertNotEqual(first, second)
        self.assertTrue(verify_password("secret", first))
        self.assertTrue(verify_password("secret", second))

    def test_column_round_trip(self):
        engine = create_engine("sqlite://")
        self.addCleanup(engine.dispose)
        metadata = MetaData()
        users = Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("password", PassWord),
        )
        metadata.create_all(engine)

        with engine.begin() as connection:
            connection.execute(
                users.insert(),
                [{"id": 1, "password": "secret"}, {"id": 2, "password": None}],
            )
            stored = connection.execute(text("SELECT password FROM users WHERE id = 1")).scalar_one()
            loaded = connection.execute(select(users.c.password).where(users.c.id == 1)).scalar_one()
            self.assertTrue(stored.startswith("$argon2id$"))
            self.assertEqual(loaded, stored)
            self.assertTrue(verify_password("secret", loaded))
            self.assertFalse(verify_password("wrong", loaded))
            self.assertIsNone(connection.execute(select(users.c.password).where(users.c.id == 2)).scalar_one())


if __name__ == "__main__":
    unittest.main()
