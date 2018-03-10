class Duck:
    def __init__(self, var) -> None:
        self.var = var

    def quack(self):
        print('Quack' + self.var)


class Dog:
    def __init__(self, var) -> None:
        super().__init__()
        self.var = var

    def bark(self):
        print('bark' + self.var)


class Quimera(Duck, Dog):
    def __init__(self, var) -> None:
        super().__init__(var)

    def hum(self):
        print('hum' + self.var)


q = Quimera('hi')
q.bark()
q.quack()
q.hum()