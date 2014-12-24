import h5scripting
import numpy as np

#
# Define the file name and provide a docstring for the whole file
# 

FileDocString = """
This file contains the data and scripts to showcase the abiities of the
h5scripting package
"""

h5_filename = "example.hdf5"

#
# Create some test data and attach it to the file.  Include docstrings
# for the group we are packing this into and also each of the arrays
# 

GroupName = "simulations/ExampleData"

DocString = r"""
This group contains some example data used in h5scripting
"""

x = np.linspace(0,10,1000)
y = np.sin(x)

# make an h5 file to test on:
with h5scripting.File(h5_filename) as f:
    
    # If we are running this a second time, clean up the existing group
    # could also do this by deleting the data one-by-one
    try: del f[GroupName]
    except: pass
    
    grp = f.require_group(GroupName, docstring = DocString)
    
    # Second order transition data
    grp["x"] = x
    grp["x"].docstring = "x: domain"

    grp["y"] = y
    grp["y"].docstring = "y: np computed sin(x)"

    
# Save a function to the h5 file

# We are leaving the two keyword arguments to attach_function as default.
# The first is 'name', the name of the dataset to save the function code to.
# Leaving it as default (None) means attach_function will use the function's
# name, so the dataset will be called 'plot_func'.
# The second is 'groupname', the group to save the dataset to. It defaults
# to 'saved_functions'.

# This decorator modifies the function to receive the h5 filename as its
# first argument, and to execute in an empty namespace.
#
# This would also be possible with
# saved_func = h5scripting.attach_function(plot_func, 
#                                          h5name, 
#                                          args=['testing calling plot_func with saved args'], 
#                                          kwargs={'xlabel': 'this is a saved keyword arg'}
#                                         )
# 
# The approach above does not modify the calling convention of plot_func
# and instead saved_func is the decoraited function

@h5scripting.attached_function(h5_filename, args=['testing calling plot_func with saved args'], kwargs={'xlabel': 'this is a saved keyword arg'})
def plot_func(h5_filename, title, xlabel='xlabel', groupname="", pdfName=None):
    """
    Plots the example data.  Notice that this docstring is stored as a 
    seperate docstring describing the function in the hdf5 file
    """
    
    import h5scripting
    import matplotlib.pyplot as plt
    
    data = h5scripting.get_all_data(h5_filename, groupname) 

    fig = plt.figure(1)
    ax = fig.add_subplot(1,1,1)
    ax.plot(data['x'], data['y'])

    ax.set_title(title)
    ax.set_xlabel(xlabel)
    
    # Generate a PDF file if requested
    if pdfName is not None:
        mpl.pyplot.savefig(pdfName, transparent=True)
    
    plt.show()
    plt.clf()
    
    return True
    
# Below we call plot_func both directly, and after retrieving it from 
# the h5 file. Both should have identical behaviour.



# You could plot_func directly. We don't provide the first
# argument, the h5 filename, as it is provided automatically:
#
# plot_func(groupname=GroupName)

#
# Now picture that you only have the h5 file that your friends sent you
#

# You can list the data within the file with documentation
for doc in h5scripting.list_all_saved_data(h5_filename):
    print(doc)

# You can list the functions attached to the file with documetation
for doc in h5scripting.list_all_saved_functions(h5_filename):
    print(doc)

    
# Here's how we retrieve plot_func from the h5 file. We're leaving
# the 'groupname' keyword argument as default, so
# get_saved_function() will look in the default group: 'saved_functions'.
retreived_plot_func = h5scripting.get_saved_function(h5_filename, 'plot_func')

# Call the retrieved function. We don't provide the first
# argument:
retreived_plot_func(groupname=GroupName, xlabel='this is a custom keyword arg')

# print out the saved function:
print(retreived_plot_func)
