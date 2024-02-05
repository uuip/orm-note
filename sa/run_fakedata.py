from sa.fakedata import setup, make
from sa.model.usercase import User, Order, GeoIp
from sa.session import s

setup(s)
make(User, 2)
make(Order, 50)
make(GeoIp, 50)
