# -*- coding: utf-8 -*-
"""
Created on Wed Jun  2 20:51:11 2021

@author: KVBA
"""
import numpy as np
import matplotlib.pyplot as plt
from shapely.affinity import translate
from scipy.spatial.distance import cdist, euclidean

def point_direction(A,B,P, plot=False):

    '''
    this function returns on which side a point lies of the line
    return -1 and 1 for either diretions

    PARAMETERS:
    - "A" and "B" points of a line segment as shapely Point object
    - "P" Point as shapely Point object of whose direction needs to be determined with respect to given line
    '''

    B_ = translate(B,xoff=-A.x, yoff=-A.y)
    P_ = translate(P,xoff=-A.x, yoff=-A.y)

    cross_product = np.cross(B_,P_)

    if plot==True:
        plt.plot([A.x, B.x],[A.y, B.y], label="Line")
        plt.scatter(P.x, P.y, label="Point", color="Red")
        plt.grid(True)
        plt.legend(loc="upper left")
        plt.show()

    if cross_product > 0:
        return 1
    elif cross_product < 0:
        return -1
    else:
        return 0

def geometric_median(X, eps=1e-5):

    '''
    This function returns a point from a list of given points such that
    the total distance of all the points from this point is minimum.

    PARAMETERS: X, list of points as tuples or list

    This function is implementation of
    Yehuda Vardi and Cun-Hui Zhang's algorithm for the geometric median,
    described in their paper "The multivariate L1-median and associated data depth".
    Link: https://www.pnas.org/content/pnas/97/4/1423.full.pdf

    Note: this implementation is only done with unweighted points
    '''
    y = np.mean(X, 0)

    while True:
        D = cdist(X, [y])
        nonzeros = (D != 0)[:, 0]

        Dinv = 1 / D[nonzeros]
        Dinvs = np.sum(Dinv)
        W = Dinv / Dinvs
        T = np.sum(W * X[nonzeros], 0)

        num_zeros = len(X) - np.sum(nonzeros)
        if num_zeros == 0:
            y1 = T
        elif num_zeros == len(X):
            return y
        else:
            R = (T - y) * Dinvs
            r = np.linalg.norm(R)
            rinv = 0 if r == 0 else num_zeros/r
            y1 = max(0, 1-rinv)*T + min(1, rinv)*y

        if euclidean(y, y1) < eps:
            return y1

        y = y1
