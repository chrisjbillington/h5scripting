# -*- coding: utf-8 -*-
"""
Created on Wed Nov  5 12:40:07 2014

@author: ispielman

This module provides an interface for saving python functions
to an h5 file and allowing these to be executed in a nice way.

One intended use of this module is embedding the function needed to generate
a plot from data within an h5 file.
"""

import h5py

def add_data(filename, groupname, data):
    """
    Adds data to a new or existing h5 file.
    
    filename : h5 file to use
    
    groupname : group to add data to.  If group exists it will be deleted first.
    
    data : a dictionary such as {"Data1": DataObject1, "Data2": DataObject2, ...}
        where the names Data1 and Data2 will be created created in group
    """

    with h5py.File(filename, "r+") as f:
        try:
            del f[groupname]
        except KeyError:
            pass

        group = f.require_group(groupname)
        
        for key, val in data.items():         
            group.create_dataset(str(key), data=val, compression="gzip")
            
            
class attach_function(object):
    """
    Saves the source of the function to an h5 file.
    
    A function decorator that saves the source of the decorated function
    as a dataset within the hdf5 file.
    
    filename : h5 file to use
    
    name : what to call the dataset to which the source is saved.
        Defaults to None, in which case the function's name will be used.
        If the dataset exists it will be deleted first.
    
    groupname : what group in the h5 file to save the dataset to.
        Defaults to 'saved_functions'.
        
    note: function should be written assuming that it enters life in
        an empty namespace.
    """
            
    def __init__(self, filename, name=None, groupname='saved_functions'):
        self.name = name
        self.filename = filename
        self.groupname = groupname
        
    def __call__(self, function):
        import inspect

        if self.name is None:
            name = function.__name__
        else:
            name = self.name
            
        function_name = function.__name__
        function_source = inspect.getsource(function)
        
        # Remove initial indentation, and this decorator (if present):
        function_lines = function_source.splitlines()
        decorator_line = '@' + self.__class__.__name__
        indentation = min(len(line) - len(line.lstrip(' ')) for line in function_lines)
        if function_lines[0][indentation:].startswith(decorator_line):
            del function_lines[0]
        function_source = '\n'.join(line[indentation:] for line in function_lines)
            
        with h5py.File(self.filename) as f:
            group = f.require_group(self.groupname)
            try:
                del group[name]
            except KeyError:
                pass
            dataset = group.create_dataset(name, data=function_source)
            dataset.attrs['function_name'] = function_name
        
        # Return a wrapped version of the function that executes
        # in a restricted environment to ensure it doesn't have
        # dependencies on global or nonlocal variables:
        return _create_sandboxed_callable(function_name, function_source)
        
        
def _create_sandboxed_callable(function_name, function_source):
    """
    Creates a callable function from the function name and source code.
    
    This callable executes in an empty namespace, and so does not have
    access to global and local variables in the calling scope.
    """
    
    import functools
    # Exec the function definition to get the function object:
    sandbox_namespace = {}
    exec function_source in sandbox_namespace
    function = sandbox_namespace[function_name]
    
    # Define a wrapped version of the function that always executes
    # in an empty namespace, so as to ensure it is self contained:
    @functools.wraps(function)
    def sandboxed_function(*args, **kwargs):
        sandbox_namespace = {'function': function, 'args': args, 'kwargs': kwargs}
        exec 'result = function(*args, **kwargs)' in sandbox_namespace
        result = sandbox_namespace['result']
        return result
    
    return sandboxed_function
        
        
def get_saved_function(filename, name, groupname='saved_functions'):
    """
    Retrieves a previouslt saved function from the h5 file.
    
    The function is returned as a callable that will run in an
    empty namespace with no access to global or local variables
    in the calling scope.
    
    filename : h5 file to use
    
    name : the name of the dataset to which the function is saved.
        if this was not set when saving the function with
        attach_function(), then this is the name of the function
        itself.
    
    groupname : the group in the h5 file to which the function is saved.
        Defaults to 'saved_functions'.
    """
    with h5py.File(filename, "r") as f:
        group = f[groupname]
        dataset = group[name]
        function_source = dataset[:]
        function_name = dataset.attrs['function_name']
    sandboxed_function = _create_sandboxed_callable(function_name, function_source)
    return sandboxed_function
    

if __name__ == '__main__':
    # tests
    
    from pylab import *
    
    x = linspace(0,10,1000)
    y = sin(x)
    some_global = 5
    
    # make an h5 file to test on:
    add_data('test.h5', 'data', dict(x=x, y=y))
        
    # test usage:
    @attach_function('test.h5')
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
    assert foo('test.h5', x=5) == True
    show()
    
    # Test we get an exception when trying
    # to access a global from foo:
    try:
        foo('test.h5', try_to_access_global=True)
    except NameError as e:
        assert repr(e) == """NameError("global name 'some_global' is not defined",)"""
    else:
        raise AssertionError('should have gotten a name error')
        
    # Success!
