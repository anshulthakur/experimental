#!python3
'''
Created on 17-Oct-2022

@author: Anshul
'''
from __future__ import print_function
from PIL import Image, ImageFilter
from numpy import asarray, array

debug = False


def progress_bar(done, total):
    print('\033[KProgress: {progress:.2f}%\r'.format(progress = (done/total)*100), end='', flush=True)

def print_debug(*args):
    global debug
    if debug:
        print(args)
    return

transform_rule = {'blue': 'white',
                  'white': 'white',
                  'black': 'white',
                  'red': 'red',
                  'green': 'green',
                  'orange': 'white'}
red_candle = ['red']
green_candle = ['green']
# transform_rule = {'blue': 'white',
#                   'white': 'white',
#                   'black': 'black',
#                   'red': 'white',
#                   'green': 'white'}

colors = {"red": (255, 0, 0),
          "green" : (0,255,0),
          "black" : (0, 0, 0),
          "white" : (255,255,255),
          "blue"  : (0, 0, 255),
          #"orange": (255,127,0)
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

def replace_colors(img):
    # Create a new image with the same size
    im2 = Image.new(img.mode, img.size, 0)
    for x in range(0, img.width):
        for y in range(0, img.height):
            # Get the pixel and its neighbors
            pixel = img.getpixel((x, y))
            im2.putpixel((x, y), 
                        colors[transform_rule[min(transform_rule.keys(), key=lambda x: sum((a - b) ** 2 for a, b in zip(colors[x], pixel)))]])
    return im2

def smoothen_neighbourhoods(im):
    from collections import Counter
    # Create a new image with the same size
    im2 = Image.new(im.mode, im.size, 0)

    # Iterate over each pixel in the image
    neighborhood_size = 1
    for x in range(neighborhood_size, im.width - neighborhood_size):
        for y in range(neighborhood_size, im.height - neighborhood_size):
            # Get the pixel and its neighbors
            pixel = im.getpixel((x, y))
            neighbors = [im.getpixel((x + dx, y + dy)) for dx, dy in [(-neighborhood_size, 0), 
                                                                      (neighborhood_size, 0), 
                                                                      (0, -neighborhood_size), 
                                                                      (0, neighborhood_size)]]

            # Check if the majority of the neighbors have the same color as the pixel
            if sum(1 for n in neighbors if n == pixel) > 2:
                # Set the pixel value in the output image
                im2.putpixel((x, y), pixel)
            else:
                # Calculate the mode color of the neighbors
                modes = Counter(neighbors).most_common(2)
                mode_color = modes[0][0]
                #if mode_color == colors['white']:
                #Edge smoothening
                if len(modes)>1 and (modes[1][1]/modes[0][1] >= 1/3):
                    mode_color = modes[1][0]

                # Find the closest color in the transform rule
                closest_color = min(transform_rule.keys(), key=lambda x: sum((a - b) ** 2 for a, b in zip(colors[x], mode_color)))

                # Set the pixel value in the output image
                im2.putpixel((x, y), colors[transform_rule[closest_color]])
    return im2

def variant2(img_name):
    base_name = img_name
    if len(img_name.split('.'))>=2 and img_name.split('.')[-1] in ['jpg', 'jpeg', 'png']:
        base_name = ''.join(img_name.split('.')[0:-1])
    else:
        img_name = img_name+'.png'
    print('Preprocessing: {}'.format(args.file))

    
    
    #Replace colors
    im = replace_colors(Image.open(img_name))
    im2 = smoothen_neighbourhoods(im)
    
    # Save the output image
    im2.save(base_name+'_crop_2.png')

def main(img_name):
    #print('Preprocessing: images/{}.png'.format(args.file))
    base_name = img_name
    if len(img_name.split('.'))>=2 and img_name.split('.')[-1] in ['jpg', 'jpeg', 'png']:
        base_name = ''.join(img_name.split('.')[0:-1])
    else:
        img_name = img_name+'.png'
    print('Preprocessing: {}'.format(img_name))
    #img = Image.open('images/'+img_name+'.png').convert('RGB')
    numpydata = array( Image.open(img_name).convert('RGB'))
    #numpydata = asarray(img)
    
    [rows, cols, _colors]= numpydata.shape
    progress = 0
    for row in range(0, rows):
        for col in range(0, cols):
            progress +=1
            color = classify(numpydata[row][col])
            if color in transform_rule:
                if (classify(numpydata[row][col]) in red_candle and classify(numpydata[row][col-1]) in green_candle)\
                    or (classify(numpydata[row][col]) in green_candle and classify(numpydata[row][col-1]) in red_candle):#Handle partitioning when boundaries touch
                    for r in range(0, rows):
                        numpydata[r][col] = colors['white']
                    continue
                else:
                    numpydata[row][col] = colors[transform_rule[color]]
            progress_bar(progress, rows*cols)
    im = Image.fromarray(numpydata)
    im.save(base_name+'_crop.png')

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Preprocess candlestick image to remove some easy artifacts')
    parser.add_argument('-f', '--file', help="Image of the candlesticks to pre-process")
    parser.add_argument('-d', '--debug', default=False, action="store_true", help="Debug traces")
    
    fname = None
    args = parser.parse_args()
    if args.file is not None and len(args.file)>0:
        fname = args.file
    if args.debug:
        print('Debug is ON')
        debug=True
    main(fname)
    #variant2(fname)
