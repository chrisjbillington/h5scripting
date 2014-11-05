from h5scripting import add_data, attach_function, get_saved_function

from pylab import *

h5_filename = 'test.h5'

x = linspace(0,10,1000)
y = sin(x)

# make an h5 file to test on:
add_data(h5_filename, 'data', dict(x=x, y=y))
    
# Save a function to the h5 file

# We are leaving the two keyword arguments to attach_function as default.
# The first is 'name', the name of the dataset to save the function code to.
# Leaving it as default (None) means attach_function will use the function's
# name, so the dataset will be called 'plot_func'.
# The second is 'groupname', the group to save the dataset to. It defaults
# to 'saved_functions'.

# This decorator modifies the function to receive the h5 filename as its
# first argument, and to execute in an empty namespace.

@attach_function(h5_filename)
def plot_func(h5_filename, title, xlabel='xlabel'):
    import h5py
    import pylab as pl
    with h5py.File(h5_filename, 'r') as f:
        x = f['/data/x'][:]
        y = f['/data/y'][:]
    pl.title(title)
    pl.xlabel(xlabel)
    pl.plot(x, y)
    return True

    
# Below we call plot_func both directly, and after retrieving it from 
# the h5 file. Both should have identical behaviour.



# Call plot_func directly. We don't provide the first
# argument, the h5 filename, as it is provided automatically:
plot_func('testing calling plot_func directly',xlabel='x (units)')
show()
clf()


# Here's how we retrieve plot_func from the h5 file. We're leaving
# the 'groupname' keyword argument as default, so
# get_saved_function() will look in the default group: 'saved_functions'.
retreived_plot_func = get_saved_function(h5_filename, 'plot_func')

# Call the retrieved function. Again, we don't provide the first
# argument:
retreived_plot_func('testing calling plot_func retrieved from file', xlabel='x (units)')
show()

