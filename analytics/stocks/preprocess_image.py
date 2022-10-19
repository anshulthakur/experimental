'''
Created on 17-Oct-2022

@author: Anshul
'''
from PIL import Image
from numpy import asarray

transform_rule = {'blue': 'white',
                  'white': 'white',
                  'black': 'black',
                  'red': 'red',
                  'green': 'green'}
colors = {"red": (255, 0, 0),
          "green" : (0,255,0),
          "black" : (0, 0, 0),
          "white" : (255,255,255),
          "blue"  : (0, 0, 255)
          }
def classify(rgb_tuple):
    #https://stackoverflow.com/questions/36439384/classifying-rgb-values-in-python
    # eg. rgb_tuple = (2,44,300)

    # add as many colors as appropriate here, but for
    # the stated use case you just want to see if your
    # pixel is 'more red' or 'more green'

    manhattan = lambda x,y : abs(x[0] - y[0]) + abs(x[1] - y[1]) + abs(x[2] - y[2]) 
    distances = {k: manhattan(v, rgb_tuple) for k, v in colors.items()}
    color = min(distances, key=distances.get)
    #if color in ['green', 'red', 'black']:
    #    print_debug(color, rgb_tuple)
    return color

def main(img_name):
    print('Preprocessing: images/{}.png'.format(args.file))
    img = Image.open('images/'+img_name+'.png').convert('RGB')
    numpydata = asarray(img)
    
    [rows, cols, _colors]= numpydata.shape
    for row in range(0, rows):
        for col in range(0, cols):
            color = classify(numpydata[row][col])
            if color in transform_rule:
                numpydata[row][col] = colors[transform_rule[color]]
    im = Image.fromarray(numpydata)
    im.save('images/'+img_name+'_crop.png')

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Preprocess candlestick image to remove some easy artifacts')
    parser.add_argument('-f', '--file', help="Image of the candlesticks to pre-process")
    
    fname = None
    args = parser.parse_args()
    if args.file is not None and len(args.file)>0:
        fname = args.file
    main(fname)