'''
Created on 31-Aug-2022

@author: Anshul
'''
from PIL import Image
from numpy import asarray
from math import floor

# load the image and convert into 
# numpy array
img = Image.open('images/2_crop.png').convert('RGB')
numpydata = asarray(img)

[rows, cols, colors]= numpydata.shape

#print(numpydata.shape)
RED=0
GREEN=1
BLUE=2
 
candles = []

debug = False
def print_debug(*args):
    if debug:
        print(args)
    return

def classify(rgb_tuple):
    #https://stackoverflow.com/questions/36439384/classifying-rgb-values-in-python
    # eg. rgb_tuple = (2,44,300)

    # add as many colors as appropriate here, but for
    # the stated use case you just want to see if your
    # pixel is 'more red' or 'more green'
    colors = {"red": (255, 0, 0),
              "green" : (0,255,0),
              "black" : (0, 0, 0),
              "white" : (255,255,255),
              }

    manhattan = lambda x,y : abs(x[0] - y[0]) + abs(x[1] - y[1]) + abs(x[2] - y[2]) 
    distances = {k: manhattan(v, rgb_tuple) for k, v in colors.items()}
    color = min(distances, key=distances.get)
    #if color in ['green', 'red', 'black']:
    #    print_debug(color, rgb_tuple)
    return color

#for row in range(0, rows):
#    print(numpydata[row, 0])
col = 0
min_close = rows
while col < cols:
    #print_debug(f'Column:{col}')
    for row in range(0, rows):
        #print_debug(f'Column:{col} Row:{row}')
        if classify(numpydata[row][col])=='black': #Black
            #Candle is starting here, now greedily scan for the entire body of the candle
            candle = {'body_start_left': [row, col]}
            print_debug(f'Body start left: {candle}')
            #Search for candle body end
            print_debug('Find bottom')
            for r in range(row, rows):
                #print_debug(r)
                colorval = classify(numpydata[r][col])
                if colorval != 'black': #Not Black
                    #Handle partial candle
                    if colorval in ['green', 'red']:
                        #print_debug('Partial candle')
                        pass
                    else:
                        candle['body_end_left'] =  [r, col]
                        print_debug(f'Body end left: {candle}')
                        break
            print_debug('Find right ends')
            for c in range(col, cols):
                #print_debug(c)
                colorval = classify(numpydata[row][c])
                if  colorval != 'black' or c==cols: #Not Black
                    candle['body_start_right']= [row, c-1]
                    candle['body_end_right'] = [r, c-1]
                    print_debug(f'Body end right: {candle}')
                    break
            #Just check some coordinate inside the candle if it was green or red
            if c==cols-1 and 'body_start_right' not in candle:
                #Reached the end of file
                break 
            candle_width = candle['body_start_right'][1] - candle['body_start_left'][1]
            if candle_width>3 and (candle['body_end_left'][0] - candle['body_start_left'][0] >= 3): #Arbitrary for now(it could be a doji)
                if classify(numpydata[row+floor((candle['body_end_left'][0] - candle['body_start_left'][0])/2)][col+floor(candle_width/2)])=='green':
                    #Green Candle body
                    candle['color'] = 'green'
                elif classify(numpydata[row+floor((candle['body_end_left'][0] - candle['body_start_left'][0])/2)][col+floor(candle_width/2)])=='red':
                    #Red Candle body
                    candle['color'] = 'red'
                elif classify(numpydata[row+floor((candle['body_end_left'][0] - candle['body_start_left'][0])/2)][col+floor(candle_width/2)])=='black':
                    #Red Candle body
                    candle['color'] = 'black'
                else:
                    print(f"Unhandled candle at {row+floor((candle['body_end_left'][0] - candle['body_start_left'][0])/2)},{col+floor(candle_width/2)}")
                    exit(0)
                if candle['color'] in ['green', 'black']:
                    candle['close'] = rows - candle['body_start_left'][0]
                    candle['open'] = rows - candle['body_end_left'][0]
                elif candle['color'] == 'red':
                    candle['close'] = rows - candle['body_end_left'][0]
                    candle['open'] = rows - candle['body_start_left'][0]
                print_debug(candle)
                min_close = min(min_close, candle['close'])
                candles.append(candle)
            elif candle_width>3 and (candle['body_end_left'][0] - candle['body_start_left'][0] < 3): #Doji
                candle['color'] = 'black'
                if candle['color'] in ['green', 'black']:
                    candle['close'] = rows - candle['body_start_left'][0]
                    candle['open'] = rows - candle['body_end_left'][0]
                elif candle['color'] == 'red':
                    candle['close'] = rows - candle['body_end_left'][0]
                    candle['open'] = rows - candle['body_start_left'][0]
                print_debug(candle)
                min_close = min(min_close, candle['close'])
                candles.append(candle)
            else:
                print_debug('Skip')
            col = c
            break
    col +=1

min_close = min_close
for candle in candles:
    candle['close'] = candle['close']/min_close
    candle['open'] = candle['open']/min_close

import datetime
from dateutil.relativedelta import relativedelta

last_candle_date = '01/08/22'
day = datetime.datetime.strptime(last_candle_date, "%d/%m/%y").date()
print(day)
#download_date = day - datetime.timedelta(days=delta)
with open('./images/2.csv', 'w') as fd:
    ii=0
    fd.write('date,Candle Color,Candle Length,open,close,change\n')
    for candle in candles:
        dayval = (day - relativedelta(months=len(candles)-ii-1)).strftime('%d/%m/%Y')
        if ii==0:
            fd.write(f"{dayval},{candle['color']},{candle['body_end_left'][0] - candle['body_start_left'][0]},{candle['open']},{candle['close']},{(candle['close']-candle['open'])/candle['close']}\n")
        else:
            fd.write(f"{dayval},{candle['color']},{candle['body_end_left'][0] - candle['body_start_left'][0]},{candle['open']},{candle['close']},{(candles[ii]['close']-candles[ii-1]['close'])/candles[ii-1]['close']}\n")
        ii+=1
    