__author__ = 'adamidesa'

import matplotlib.pyplot as plt
import numpy as np


def plot():
    x=[1,2,3,4,5,6,7,8,9,10]
    y=[5,8,100,120,20,200,300,10,350,170]
    plt.ylim((0,300))
    plt.plot(x,y,'r.-')
    plt.xlabel("Features")
    plt.ylabel("")
    plt.show()



if __name__ == '__main__':
    print("Starting processing...")
    plot()
