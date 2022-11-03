from dataclasses import dataclass


@dataclass
class AddressAggregate:
    in_: int = 0
    out: int = 0

    def __str__(self):
        return f'in: {self.in_}, out: {self.out}, sum: {self.sum}'

    @property
    def sum(self):
        return self.in_ + self.out