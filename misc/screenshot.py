#!/home/craft/tests/experimental/env/bin/python

import pyscreenshot as ImageGrab
from datetime import datetime as time
from time import sleep

def capture():
  while(True):
    sleep(5)
    im=ImageGrab.grab()
    im = im.convert('L')
    im.save('./screens/{time}.png'.format(time=time.now()))
    im.close()
    del(im)

if __name__=='__main__':
  capture()

