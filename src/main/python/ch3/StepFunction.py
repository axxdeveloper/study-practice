import numpy as np
import matplotlib.pylab as plt


def step_function_1(x):
    if x > 0:
        return 1
    else:
        return 0


def step_function_2(x):
    y = x > 0
    return y.astype(np.int)


def step_function(x):
    return np.array(x > 0, dtype=np.int)



if __name__ == '__main__':
    x = np.arange(-5.0,5.0,0.1)
    y = step_function(x)
    print(y)
    plt.plot(x,y)
    plt.ylim(-0.1,1.1)
    plt.show()