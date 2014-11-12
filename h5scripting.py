# -*- coding: utf-8 -*-
"""
Created on Wed Nov  5 12:40:07 2014

@author: ispielman

This module provides an interface for saving python functions
to an h5 file and allowing these to be executed in a nice way.

One intended use of this module is embedding the function needed to generate
a plot from data within an h5 file.
"""

import os
import sys
import ast

import h5py


def exec_in_namespace(code, namespace):
    if sys.version < '3':
        exec("""exec code in namespace""")
    else:
        if isinstance(__builtins__, dict):
            exec_func = __builtins__['exec']
        else:
            exec_func = getattr(__builtins__, 'exec')
        exec_func(code, namespace)


def add_data(filename, groupname, data, docstring = None):
    """
    Adds data to a new or existing h5 file.

    filename : h5 file to use

    groupname : group to add data to.  If group exists it will be deleted first.

    data : a dictionary such as {"Data1": DataObject1, "Data2": DataObject2, ...}
        where the names Data1 and Data2 will be created created in group
    
    docstring : if passed, will be added as an attribute to the group
        this is intended to be a string note describing the contents
        of a folder, for example,  but it could be anything.
    
    Adds an attribute "__h5scripting" to all data added to allow for
    safe batch readout of the data.
    """

    with h5py.File(filename) as f:
        try:
            del f[groupname]
        except KeyError:
            pass

        group = f.require_group(groupname)
        if docstring is not None:
            group.attrs['group_docstring'] = docstring

        for key, val in data.items():
            dataset = group.create_dataset(str(key), data=val, compression="gzip")
            dataset.attrs['__h5scripting_data'] = True

            
def get_data(filename, groupname):
    """
    Gets data from an existing h5 file.

    filename : h5 file to use

    groupname : group to use
    
    only datasets with the "__h5scripting_data" attribute set to True are accepted

    returns : a dictionary such as {"Data1": DataObject1, "Data2": DataObject2, ...}
        where the names are the h5 dataset names.
    """

    h5data = {}
    with h5py.File(filename, 'r') as f:
        grp = f[groupname]
        for dataset in grp.values():
            # First check that this is a valid data.
            if '__h5scripting_data' in dataset.attrs and dataset.attrs['__h5scripting_data']:
                key = dataset.name
                key = key.split("/")[-1]
                h5data[key] = dataset.value

    return h5data

    
class attached_function(object):

    """
    Decorator that saves the decorated function to an h5 file.

    A function decorator that saves the source of the decorated function
    as a dataset within the hdf5 file, along with other data for how the
    function should be called.

    filename : h5 file to use. This will be passed to automatically
        to the saved function as its first argument.

    name : what to call the dataset to which the source is saved.
        Defaults to None, in which case the function's name will be used.
        If the dataset exists it will be deleted first.

    docstring : a string describing this function, or the data it is plotting
        or whatever.  if this is None or not passed this is pulled from
        the function's docstring.

    groupname : what group in the h5 file to save the dataset to.
        Defaults to 'saved_functions'.
        
    args : list or tuple of arguments that will be automatically passed
        to the function, after the filename argument.
        
    kwargs: dictionary of keyword arguments that will be automatically passed
        to the function.

    note: function should be written assuming that it enters life in
        an empty namespace. This decorator modifies the defined function
        to run in an empty namespace, and to be called with the provided
        arguments and keyword arguments.
    """

    def __init__(self, filename, name=None, docstring=None, groupname='saved_functions', args=None, kwargs=None):
        self.name = name
        self.filename = filename
        self.groupname = groupname
        self.docstring = docstring
        self.args = args
        self.kwargs = kwargs
        
    def __call__(self, function):
        import inspect
        
        if self.name is None:
            name = function.__name__
        else:
            name = self.name

        function_name = function.__name__

        if self.docstring is not None:
            function_docstring = (
                "\n----- DATA DOCSTRING -----\n" +
                self.docstring)
        else:
             function_docstring = ""
        
        if function.__doc__ is not None:
            function_docstring += (
                    "\n----- FUNCTION DOCSTRING -----\n" + 
                    function.__doc__)

        if function_docstring is None:
            function_docstring = ""

        if self.args is None:
            args = []
        else:
            args = self.args
        function_args = repr(args)
        if not ast.literal_eval(function_args) == args:
            raise ValueError('Argument list can contain only Python literals')
        
        if self.kwargs is None:
            kwargs = {}
        else:
            kwargs = self.kwargs
        function_kwargs = repr(kwargs)
        if not ast.literal_eval(function_kwargs) == kwargs:
            raise ValueError('Keyword argument list can contain only Python literals')
            
        argspec = inspect.getargspec(function)
        function_signature = function_name + inspect.formatargspec(*argspec)
        function_source = inspect.getsource(function)
        
        function_lines = function_source.splitlines()
        indentation = min(len(line) - len(line.lstrip(' ')) for line in function_lines)
        # Remove this decorator from the source, if present:
        if function_lines[0][indentation:].startswith('@'):
            del function_lines[0]
        # Remove initial indentation from the source:
        function_source = '\n'.join(line[indentation:] for line in function_lines)

        with h5py.File(self.filename) as f:
            group = f.require_group(self.groupname)
            try:
                del group[name]
            except KeyError:
                pass
            dataset = group.create_dataset(name, data=function_source)
            dataset.attrs['function_name'] = function_name
            dataset.attrs['function_docstring'] = function_docstring
            dataset.attrs['function_signature'] = function_signature
            dataset.attrs['function_args'] = function_args
            dataset.attrs['function_kwargs'] = function_kwargs
            dataset.attrs['__h5scripting_function'] = True
            
            saved_function = SavedFunction(dataset)
        return saved_function


def attach_function(function, filename, name=None, docstring=None, groupname='saved_functions', args=None, kwargs=None):
    """
    Saves the source of a function to an h5 file.

    This is exactly the same as the attached_function decorator, except
    that one passes in the function to be saved as the firt argument instead
    of decorating its definition. Returns the sandboxed version of the function.
    
    function : The function to save

    All other arguments are the same as in the attached_function decorator.
    
    note: The function's source code must be self contained and introspectable
        by Python, that means no lambdas, class/instance methods, functools.partial
        objects, C extensions etc, only ordinary Python functions.
    """
    attacher = attached_function(filename, name, docstring, groupname, args, kwargs)
    saved_function = attacher(function)
    return saved_function
 

class SavedFunction(object):
    def __init__(self, dataset):
        """provides a callable from the function saved in the provided dataset.
        
        filename: The name of the (currently open) h5 file the 
        
        This callable executes in an empty namespace, and so does not have
        access to global and local variables in the calling scope.

        When called, it automatically receives 'filename' as its first
        argument, args and kwargs as its arguments and keyword arguments."""
        
        import functools
        
        function_source = dataset.value
        function_name = dataset.attrs['function_name']
        function_docstring = dataset.attrs['function_docstring']
        function_signature = dataset.attrs['function_signature']
        function_args = ast.literal_eval(dataset.attrs['function_args'])
        function_kwargs = ast.literal_eval(dataset.attrs['function_kwargs'])
        
        # Exec the function definition to get the function object:
        sandbox_namespace = {}
        exec_in_namespace(function_source, sandbox_namespace)
        function = sandbox_namespace[function_name]
    
        self._function = function
        self.function_docstring = function_docstring
        self.function_signature = function_signature
        self.function_source = function_source
        self.function_name = function_name
        self.function_args = function_args
        self.function_kwargs = function_kwargs
        self.h5_filename = os.path.abspath(dataset.file.filename)
        functools.update_wrapper(self, function)
        
    def __call__(self, *args, **kwargs):
        """Calls the wrapped function in an empty namespace. Returns the result.
        If keyword arguments are provided, these override the saved keyword arguments.
        Positional arguiments cannot be overridden, please use custom_call() for that.."""
        if args:
            message = ("To call this SavedFunction with custom positional arguments, please call  the custom_call()', " +
                       "method, passing in all desired arguments and keyword arguments.")
            raise TypeError(message)
        sandbox_kwargs = self.function_kwargs.copy()
        sandbox_kwargs.update(kwargs)
        return self.custom_call(*self.function_args, **sandbox_kwargs)
            
    def custom_call(self, *args, **kwargs):
        """Call the wrapped function with custom positional and keyword arguments."""
        # Names mangled to reduce risk of colliding with the function
        # attempting to access global variables (which it shouldn't be doing):
        sandbox_namespace = {'__h5s_filename': self.h5_filename,
                             '__h5s_function': self._function,
                             '__h5s_args': args,
                             '__h5s_kwargs': kwargs}
        exc_line = '__h5s_result = __h5s_function(__h5s_filename, *__h5s_args, **__h5s_kwargs)'
        exec_in_namespace(exc_line, sandbox_namespace)
        result = sandbox_namespace['__h5s_result']
        return result
        
    def __repr__(self):
        """A pretty representation of the object that displays all public attributes"""
        function_source = self.function_source.splitlines()[0]
        if len(function_source) > 50:
            function_source = function_source[:50] + '...'
        function_docstring = self.function_docstring
        if self.function_docstring:
            function_docstring = str(self.function_docstring).splitlines()[0]
            if len(function_docstring) > 50:
                function_docstring = function_docstring[:50] + '...'
        function_args = repr(self.function_args).splitlines()[0]
        if len(function_args) > 50:
            function_args = function_args[:50] + '...'
        function_kwargs = repr(self.function_kwargs).splitlines()[0]
        if len(function_kwargs) > 50:
            function_kwargs = function_kwargs[:50] + '...'
        return ('<%s:\n'%self.__class__.__name__ +
                '    function_name=%s\n'%self.function_name + 
                '    function_source=%s\n'%function_source +
                '    function_docstring=%s\n'%function_docstring + 
                '    function_args=%s\n'%function_args + 
                '    function_kwargs=%s\n'%function_kwargs + 
                '    h5_filename=%s>'%self.h5_filename) 
        
        
def get_saved_function(filename, name, groupname='saved_functions'):
    """
    Retrieves a previously saved function from the h5 file.

    The function is returned as a callable that will run in an
    empty namespace with no access to global or local variables
    in the calling scope.

    filename : h5 file to use

    name : the name of the dataset to which the function is saved.
        if this was not set when saving the function with
        attach_function() or attached_function(), then this
        is the name of the function itself.

    groupname : the group in the h5 file to which the function is saved.
        Defaults to 'saved_functions'
        
    returns saved_function
    """

    with h5py.File(filename, "r") as f:
        group = f[groupname]
        dataset = group[name]
        if '__h5scripting_function' not in dataset.attrs or not dataset.attrs['__h5scripting_function']:
            raise ValueError('Specified dataset does not represent a function saved with h5scripting.')
        saved_function = SavedFunction(dataset)
    
    return saved_function


def get_all_saved_functions(filename, groupname='saved_functions'):
    """
    returns all the saved functions in the group deined by groupname as 
    a list of the form:
    
    [saved_function, ]
    
    This assumes that all of the datasets in groupname are saved functions.
    """
    with h5py.File(filename, "r") as f:
        group = f[groupname]
        keys = group.keys()
        
        saved_functions = []
        for key in keys:
            dataset = group[key]
            if '__h5scripting_function' in dataset.attrs and dataset.attrs['__h5scripting_function']:
                saved_functions += [SavedFunction(dataset),]

    return saved_functions

