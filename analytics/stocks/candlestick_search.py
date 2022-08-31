'''
Created on 31-Aug-2022

@author: Anshul
'''
from PIL import Image
from numpy import asarray
from math import floor

# load the image and convert into 
# numpy array
img = Image.open('images/1.png').convert('RGB')
numpydata = asarray(img)

[rows, cols, colors]= numpydata.shape

#print(numpydata.shape)
RED=0
GREEN=1
BLUE=2
 
candles = []

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
    print(color, rgb_tuple)
    return color

#for row in range(0, rows):
#    print(numpydata[row, 0])
for col in range(0, cols):
    print(f'Column:{col}')
    for row in range(0, rows):
        print(f'Column:{col} Row:{row}')
        if classify(numpydata[row][col])=='black': #Black
            #Candle is starting here, now greedily scan for the entire body of the candle
            candle = {'body_start_left': [row, col]}
            print(f'Body start left: {candle}')
            #Search for candle body end
            print('Find bottom')
            for r in range(row, rows):
                print(r)
                colorval = classify(numpydata[r][col])
                if colorval != 'black': #Not Black
                    #Handle partial candle
                    if colorval in ['green', 'red']:
                        print('Partial candle')
                    else:
                        candle['body_end_left'] =  [r, col]
                        print(f'Body end left: {candle}')
                        break
            print('Find right ends')
            for c in range(col, cols):
                print(c)
                colorval = classify(numpydata[row][c])
                if  colorval != 'black' or c==cols: #Not Black
                    candle['body_start_right']= [row, c]
                    candle['body_end_right'] = [r, c]
                    print(f'Body end right: {candle}')
                    break
            #Just check some coordinate inside the candle if it was green or red
            candle_width = candle['body_start_right'][1] - candle['body_start_left'][1] 
            if (candle['body_end_left'][0] - candle['body_start_left'][0] >= 3): #Arbitrary for now(it could be a doji)
                if classify(numpydata[row+3][floor(candle_width/2)])=='green':
                    #Green Candle body
                    candle['color'] = 'green'
                elif classify(numpydata[row+3][floor(candle_width/2)])=='red':
                    #Red Candle body
                    candle['color'] = 'red'
            print(candle)
            candles.append(candle)
            continue
