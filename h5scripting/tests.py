from h5scripting import add_data, attached_function, get_saved_function, get_all_saved_functions

from pylab import *

TEST_FILENAME = 'test.h5'


x = linspace(0,10,1000)
y = sin(x)
some_global = 5


# make an h5 file to test on:
add_data(TEST_FILENAME, 'data', dict(x=x, y=y))
    
    
# Save a function to the h5 file (leaving default name and group kwargs).
# This decorator modifies the function to receive the h5 filepath as its
# first argument, and to execute in an empty namespace.
@attached_function(TEST_FILENAME)
def foo(h5_filepath, try_to_access_global=False, **kwargs):
    import h5py
    import pylab as pl
    with h5py.File(h5_filepath, 'r') as f:
        x = f['/data/x'][:]
        y = f['/data/y'][:]
    pl.xlabel('kwargs are: %s'%str(kwargs))
    pl.plot(x, y)
    if try_to_access_global:
        print(some_global)
    return True

    
# Test that we can call foo,
# that it returns the right value,
# and that we can show the plot:
assert foo(x=5) == True
show()
clf()

# Test we get an exception when trying
# to access a global from foo:
try:
    foo(try_to_access_global=True)
except NameError as e:
    assert repr(e) in ["""NameError("name 'some_global' is not defined",)""",
                       """NameError("global name 'some_global' is not defined",)"""]
else:
    raise AssertionError('should have gotten a name error')
 
 
# Test that we can retrieve foo from the h5 file and call it
retreived_foo = get_saved_function(TEST_FILENAME, 'foo')
assert retreived_foo(x=5) == True
show()

import pprint
pprint.pprint(get_all_saved_functions(TEST_FILENAME))
