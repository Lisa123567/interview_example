class Kettle(object):
    power_source = "electricity"

    def __init__(self, make, price):
        self.make = make
        self.price = price
        self.on = False

    def switch_on(self):
        self.on = True

volga = Kettle("Volga", 15.2)
zigul = Kettle("Zigul", 16.08)
mersedess = Kettle("Mersedess", 15.55)

print ("Models:\n {0.make} = {0.price}, {1.make} = {1.price}, {2.make} = {2.price}".format(volga,zigul,mersedess))
print("=" * 40)
print (volga.on)
volga.switch_on()
print(volga.on)
print("=" * 40)
print(Kettle.power_source)
print(volga.power_source)
print(mersedess.power_source)
print("=" * 40)
Kettle.power_source = "atomic"
print(Kettle.power_source)
print(volga.power_source)
print(mersedess.power_source)
print("=" * 40)
Kettle.power_source = "gas"
print(Kettle.power_source)
print(volga.power_source)
print(mersedess.power_source)
print("=" * 40)
print(Kettle.__dict__)
print(volga.__dict__)
print(mersedess.__dict__)


volga.power = 1.5
print(volga.power)
print(mersedess.power)