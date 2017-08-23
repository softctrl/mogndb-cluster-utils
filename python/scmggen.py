# -*- coding: utf-8 -*-

"""
scmggen.py

I am developing this program to help-me at my work.
I hope that this project may help you too.
But, i will not be responsible for any trouble that you may have.
You are by your own.

If you do not have knowledge so is better you do not use this.

Copyright (c) 2017 Carlos Timoshenko Rodrigues Lopes carlostimoshenkorodrigueslopes@gmail.com
http://www.0x09.com.br

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


"""

import sys
import json
from pymongo import MongoClient, ASCENDING, DESCENDING


def inc_c(c):
    return chr(ord(c) + 1)


class Plate:
    def __init__(self, a1, a2, a3, n):
        self.a1 = a1
        self.a2 = a2
        self.a3 = a3
        self.n = n

    def __str__(self):
        return '%c%c%c%04d' % (self.a1, self.a2, self.a3, self.n)

    def next(self):
        if self.n < 9999:
            self.n = self.n + 1
        else:
            self.n = 0
            if ord(self.a3) < ord('z'):
                self.a3 = inc_c(self.a3)
            else:
                self.a3 = 'a'
                if ord(self.a2) < ord('z'):
                    self.a2 = inc_c(self.a2)
                    if self.a2 == 'z':  # TODO: remove on production
                        self.a2 = 'a'
                else:
                    self.a2 = 'a'
                    if ord(self.a1) < ord('z'):
                        self.a1 = inc_c(self.a1)
                    else:
                        None  # self.a1 = 'a'
        return self


if __name__ == '__main__':
    plate = Plate('a', 'a', 'a', 0)
    for x in range(0, 1000000):
        print("Plate: " + str(plate))
        _ = plate.next()
