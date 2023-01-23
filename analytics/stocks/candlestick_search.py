'''
Created on 31-Aug-2022

@author: Anshul
'''
from PIL import Image
from numpy import asarray
from math import floor
import pandas as pd
import matplotlib.pyplot as plt

#print(numpydata.shape)
RED=0
GREEN=1
BLUE=2

debug = False
def print_debug(*args):
    global debug
    if debug:
        print(args)
    return

color_scheme = {'candlesticks': {
                                'body': ['white', 'black'],
                                #'border': ['green', 'red'],
                                'border': ['black']
                                },
                'bars': {
                    'body': ['green', 'red']
                }
                }

candle_color_options = {
    'green': ['green', 'white'],
    'red': ['red', 'black']
}

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

def main(filename, ohlc_type='candlestick', find_tops=False):
    # load the image and convert into 
    # numpy array
    img_name = filename
    img = Image.open('images/'+img_name+'.png').convert('RGB')
    numpydata = asarray(img)
    candles = []

    
    [rows, cols, colors]= numpydata.shape
    #for row in range(0, rows):
    #    print(numpydata[row, 0])
    col = 0
    min_close = rows
    max_close = 0
    while col < cols:
        #print_debug(f'Column:{col}')
        for row in range(0, rows):
            #print_debug(f'Column:{col} Row:{row}')
            if ohlc_type=='candlestick' and classify(numpydata[row][col]) in color_scheme['candlesticks']['border']:
                #Candle is starting here, now greedily scan for the entire body of the candle
                candle = {'body_start_left': [row, col]}
                print_debug(f'Body start left: {candle}')
                #Search for candle body end
                print_debug('Find bottom')
                for r in range(row, rows):
                    #print_debug(r)
                    colorval = classify(numpydata[r][col])
                    if colorval not in color_scheme['candlesticks']['border']: #Not Black
                        candle['body_end_left'] =  [r, col]
                        print_debug(f'Body end left: {candle}')
                        break
                print_debug('Find right ends')
                for c in range(col, cols):
                    #print_debug(c)
                    colorval = classify(numpydata[row][c])
                    if  colorval not in color_scheme['candlesticks']['border'] or c==cols: #Not Black
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
                    if classify(numpydata[row+floor((candle['body_end_left'][0] - candle['body_start_left'][0])/2)][col+floor(candle_width/2)]) in candle_color_options['green']:
                        #Green Candle body
                        candle['color'] = 'green'
                    elif classify(numpydata[row+floor((candle['body_end_left'][0] - candle['body_start_left'][0])/2)][col+floor(candle_width/2)]) in candle_color_options['red']:
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
                    max_close = max(max_close, candle['close'])
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
                    max_close = max(max_close, candle['close'])
                    candles.append(candle)
                else:
                    print_debug('Skip')
                col = c
                break
            elif ohlc_type=='bars' and classify(numpydata[row][col]) in ['green', 'red', 'black'] and not find_tops:
                #Candle is starting here, now greedily scan for the entire body of the candle
                if classify(numpydata[row][col])=='green':
                    #Scan upwards
                    candle = {'body_end_left': [row+2, col]}
                    candle['color'] = 'green'
                    print_debug(f'Body end left: {candle}')
                    for c in range(col, cols):
                        #print_debug(c)
                        colorval = classify(numpydata[row][c])
                        if  colorval != 'green' and c<=cols: #Could be reaching the end of bar width (edge case of doji)
                            #Try to find green in rows above
                            #print_debug("Find green top after col {}".format(c))
                            for r in range(row, 0, -1):
                                colorval = classify(numpydata[r][c])
                                if  colorval == 'green':
                                    candle['body_start_left']= [r-2, col]
                                    candle['body_end_right'] = [row+2, c-1]
                                    candle['body_start_right'] = [r-2, c-1]
                                    print_debug(f'Body end right: {candle}')
                                    break
                            if 'body_start_right' not in candle:
                                #Doji case
                                candle['body_start_left']= [row-2, col]
                                candle['body_end_right'] = [row+2, c-1]
                                candle['body_start_right'] = [row-2, c-1]
                                print_debug(f'Doji: Body end right: {candle}')
                                col = c
                            else: #Reach end of green top bar
                                col = c
                                for c in range(col, cols):
                                    colorval = classify(numpydata[r][c])
                                    if  colorval != 'green' and c<=cols:
                                        col = c
                                        break
                            break
                elif classify(numpydata[row][col])=='red':
                    #Scan downwards
                    candle = {'body_start_left': [row, col]}
                    candle['color'] = 'red'
                    print_debug(f'Body start left: {candle}')
                    for c in range(col, cols):
                        #print_debug(c)
                        colorval = classify(numpydata[row][c])
                        if  colorval != 'red' and c<=cols: #Could be reaching the end of bar width (edge case of doji)
                            #Try to find red in rows below
                            #print_debug("Find red bottom after col {}".format(c))
                            for r in range(row, rows):
                                colorval = classify(numpydata[r][c])
                                if  colorval == 'red':
                                    candle['body_end_left']= [r+2, col]
                                    candle['body_start_right'] = [row, c-1]
                                    candle['body_end_right'] = [r+2, c-1]
                                    print_debug(f'Body end right: {candle}')
                                    break
                            if 'body_start_right' not in candle:
                                #Doji case
                                candle['body_end_left']= [row+2, col]
                                candle['body_end_right'] = [row+2, c-1]
                                candle['body_start_right'] = [row, c-1]
                                print_debug(f'Doji: Body end right: {candle}')
                                col = c
                            else: #Reach end of red top bar
                                col = c
                                for c in range(col, cols):
                                    colorval = classify(numpydata[r][c])
                                    if  colorval != 'red' and c<=cols:
                                        col = c
                                        break
                            break
                elif classify(numpydata[row][col])=='black':
                    #doji
                    candle = {'body_start_left': [row, col]}
                    candle['color'] = 'black'
                    print_debug(f'Body start left: {candle}')
                    for c in range(col, cols):
                        #print_debug(c)
                        colorval = classify(numpydata[row][c])
                        if  colorval != 'black' and c<=cols: #Could be reaching the end of bar width (edge case of doji)
                            candle['body_end_left']= [row+2, col]
                            candle['body_end_right'] = [row+2, c-1]
                            candle['body_start_right'] = [row, c-1]
                            col = c
                            
                            break
                candle_width = candle['body_start_right'][1] - candle['body_start_left'][1]
                if candle_width>=2:
                    if candle['color'] in ['green', 'black']:
                        candle['close'] = rows - candle['body_start_left'][0]
                        candle['open'] = rows - candle['body_end_left'][0]
                    elif candle['color'] == 'red':
                        candle['close'] = rows - candle['body_end_left'][0]
                        candle['open'] = rows - candle['body_start_left'][0]
                    print_debug(candle)
                    min_close = min(min_close, candle['close'])
                    max_close = max(max_close, candle['close'])
                    candles.append(candle)
                else:
                    print_debug('Skip')
                col = c
                break
            elif ohlc_type=='bars' and classify(numpydata[row][col]) in ['green', 'red', 'black'] and find_tops:
                #Candle is starting here, now greedily scan for the entire body of the candle
                if classify(numpydata[row][col])=='green':
                    #Scan downwards
                    candle = {'body_start_left': [row, col]}
                    candle['color'] = 'green'
                    print_debug(f'Body start left: {candle}')
                    for c in range(col, cols):
                        #print_debug(c)
                        colorval = classify(numpydata[row][c])
                        if  colorval != 'green' and c<=cols: #Could be reaching the end of bar width (edge case of doji)
                            #Try to find green in rows below
                            #print_debug("Find green bottom after col {}".format(c))
                            candle['body_start_right'] =  [row, c-1]
                            for r in range(row, rows):
                                colorval = classify(numpydata[r][c-1])
                                if  colorval != 'green':
                                    candle['body_end_left']= [r-1, col]
                                    candle['body_end_right'] = [r-1, c-1]
                                    print_debug(f'Body end right: {candle}')
                                    break
                            if 'body_start_right' not in candle:
                                #Doji case
                                candle['body_end_left']= [row+2, col]
                                candle['body_end_right'] = [row+2, c-1]
                                print_debug(f'Doji: Body end right: {candle}')
                                col = c
                            else: #Reach end of green top bar
                                col = c
                                for c in range(col, cols):
                                    colorval = classify(numpydata[r][c])
                                    if  colorval != 'green' and c<=cols:
                                        col = c
                                        break
                            break
                elif classify(numpydata[row][col])=='red':
                    #Scan downwards
                    candle = {'body_start_left': [row, col]}
                    candle['color'] = 'red'
                    print_debug(f'Body start left: {candle}')
                    for c in range(col, cols):
                        #print_debug(c)
                        colorval = classify(numpydata[row][c])
                        if  colorval != 'red' and c<=cols: #Could be reaching the end of bar width (edge case of doji)
                            #Try to find green in rows below
                            #print_debug("Find green bottom after col {}".format(c))
                            candle['body_start_right'] = [row, c-1]
                            for r in range(row, rows):
                                colorval = classify(numpydata[r][c-1])
                                if  colorval != 'red':
                                    candle['body_end_left']= [r-1, col]
                                    candle['body_end_right'] = [r-1, c-1]
                                    print_debug(f'Body end right: {candle}')
                                    break
                            if 'body_start_right' not in candle:
                                #Doji case
                                candle['body_end_left']= [row+2, col]
                                candle['body_end_right'] = [row+2, c-1]
                                print_debug(f'Doji: Body end right: {candle}')
                                col = c
                            else: #Reach end of green top bar
                                col = c
                                for c in range(col, cols):
                                    colorval = classify(numpydata[r][c])
                                    if  colorval != 'red' and c<=cols:
                                        col = c
                                        break
                            break
                elif classify(numpydata[row][col])=='black':
                    #doji
                    candle = {'body_start_left': [row, col]}
                    candle['color'] = 'black'
                    print_debug(f'Body start left: {candle}')
                    for c in range(col, cols):
                        #print_debug(c)
                        colorval = classify(numpydata[row][c])
                        if  colorval != 'black' and c<=cols: #Could be reaching the end of bar width (edge case of doji)
                            candle['body_end_left']= [row+2, col]
                            candle['body_end_right'] = [row+2, c-1]
                            candle['body_start_right'] = [row, c-1]
                            col = c
                            break
                candle_width = candle['body_start_right'][1] - candle['body_start_left'][1]
                if candle_width>=1:
                    if candle['color'] in ['green', 'black']:
                        candle['close'] = rows - candle['body_start_left'][0]
                        candle['open'] = rows - candle['body_end_left'][0]
                    elif candle['color'] == 'red':
                        candle['close'] = rows - candle['body_end_left'][0]
                        candle['open'] = rows - candle['body_start_left'][0]
                    print_debug(candle)
                    min_close = min(min_close, candle['close'])
                    max_close = max(max_close, candle['close'])
                    candles.append(candle)
                else:
                    print_debug('Skip')
                col = c
                break
        col +=1
    
    #Normalize to range value
    min_close = min_close
    print(f'Max candle close: {max_close}. Min candle close: {min_close}')
    for candle in candles:
        candle['close'] = candle['close']/(max_close - min_close)
        candle['open'] = candle['open']/(max_close - min_close)
    
    import datetime
    from dateutil.relativedelta import relativedelta
    
    last_candle_date = '05/09/22'
    day = datetime.datetime.strptime(last_candle_date, "%d/%m/%y").date()
    print(day)
    #download_date = day - datetime.timedelta(days=delta)
    with open('./images/'+img_name+'.csv', 'w') as fd:
        ii=0
        fd.write('index,date,Candle Color,Candle Length,open,close,change\n')
        for candle in candles:
            #dayval = (day - relativedelta(months=len(candles)-ii-1)).strftime('%d/%m/%Y')
            dayval = (day - relativedelta(weeks=len(candles)-ii-1)).strftime('%d/%m/%Y')
            if ii==0:
                fd.write(f"{ii},{dayval},{candle['color']},{candle['body_end_left'][0] - candle['body_start_left'][0]},{candle['open']},{candle['close']},{(candle['close']-candle['open'])/candle['close']}\n")
            else:
                fd.write(f"{ii},{dayval},{candle['color']},{candle['body_end_left'][0] - candle['body_start_left'][0]},{candle['open']},{candle['close']},{(candles[ii]['close']-candles[ii-1]['close'])/candles[ii-1]['close']}\n")
            ii+=1
            
    r_df = pd.read_csv('./images/'+img_name+'.csv')
    r_df.reset_index(inplace = True)
    r_df['date'] = pd.to_datetime(r_df['date'], format='%d/%m/%Y')
    r_df.set_index('date', inplace = True)
    r_df = r_df.sort_index()
    r_df.plot(y='close')
    plt.savefig('./images/'+img_name+'_line.png')
    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Perform reverse search for indices')
    parser.add_argument('-f', '--file', help="Image file of the candlesticks to parse for")
    parser.add_argument('-t', '--type', default='candlestick', help="Type of OHLC plot (candlestick/bars)")
    parser.add_argument('-d', '--debug', default=False, action="store_true", help="Debug traces")
    parser.add_argument('-p', '--peak', default=False, action="store_true", help="Debug traces")
    
    #Can add options for weekly sampling and monthly sampling later
    args = parser.parse_args()
    if args.file is not None and len(args.file)>0:
        print('Search stock for file: {}'.format(args.file))
    else:
        print('Speficy file name.')
        exit(0)
    if args.debug:
        print('Debug is ON')
        debug=True
    main(filename=args.file, ohlc_type=args.type, find_tops=args.peak)
