import numpy
import numpy as np
from PIL import Image
import cv2


if __name__=='__main__':
    img =cv2.imread('400 bad request.png',cv2.IMREAD_GRAYSCALE)

    th, img_b = cv2.threshold(img, 128, 255, cv2.THRESH_OTSU)

    #print(th)
    # 117.0

    #cv2.imwrite('spark1.png', img_b)

    #img = cv2.imread('spark1.png', cv2.IMREAD_GRAYSCALE)

    print(img_b.shape)
    #print(img_b[0])
    #print(img_b[0][0])
    '''
    res=[]
    for i in range(img_b.shape[0]):
        for j in range(img_b.shape[1]):
            if(img_b[i][j] < 100):
                map={}
                map['x'] = 300-i
                map['y'] = 168-j
                map['v'] = 1
                res.append(map)
    print(res)
    '''
    res = []
    i=0
    k=0
    while(i < img_b.shape[1]-1):
        j=img_b.shape[0]-1
        while(j > 0):
            if (img_b[j][i] < 100):
                if(k % 10 == 0):
                    k+=1
                    map={}
                    map['x'] = i
                    map['y'] = img_b.shape[0]-j
                    map['r'] = 5
                    res.append(map)
                else:
                    k+=1
            j=j-1
        i=i+1


    with open('400.txt', 'w') as fp :
        fp.write("%s" %  res)
        #for r in res: fp.write("%s," % r)

    '''
    
    for i in range(img_b.shape[0]):
        s=''
        for j in range(img_b.shape[1]):
            if (img_b[i][j] < 100) :
                s= s +'1'
            else:
                s= s +'0'
        #print(s+'\n')
    '''



