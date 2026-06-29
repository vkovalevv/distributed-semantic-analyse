import numpy as np 

N=[1,2,3,4,5,6]
S=[1.00,1.77,2.47,3.02,3.69,4.29]
x=[1/n for n in N]
y=[1/s_ for s_ in S]
slope, intercept = np.polyfit(x,y,1)
print('s=',intercept)