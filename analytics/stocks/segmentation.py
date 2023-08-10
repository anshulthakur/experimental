import cv2
import numpy as np
image=cv2.imread('images/19.png')
cv2.imshow('input image',image)
cv2.waitKey(0)

gray=cv2.cvtColor(image,cv2.COLOR_BGR2GRAY)

edged=cv2.Canny(gray,30,200)
cv2.imshow('canny edges',edged)
cv2.waitKey(0)

contours, hierarchy=cv2.findContours(edged,cv2.RETR_EXTERNAL,cv2.CHAIN_APPROX_NONE)
cv2.imshow('canny edges after contouring', edged)
cv2.waitKey(0)

#use -1 as the 3rd parameter to draw all the contours
cv2.drawContours(image,contours,-1,(10,10,10),3)
cv2.imshow('contours',image)
cv2.waitKey(0)

cv2.imwrite('images/19_processed.png', image)
#Save image
cv2.destroyAllWindows()

