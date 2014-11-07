# -*- coding: utf-8 -*-
"""
Created on Wed Nov  5 12:40:07 2014

@author: ispielman

This module provides an interface for saving python functions
to an h5 file and allowing these to be executed in a nice way.

One intended use of this module is embedding the function needed to generate
a plot from data within an h5 file.
"""

import sys
import h5py
import ast

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
            dataset.attrs['__h5scripting'] = True

def get_data(filename, groupname, recursive = False):
    """
    Gets data from an existing h5 file.

    filename : h5 file to use

    groupname : group to use

    recursive : search the data tree recursivly.
    
    only datasets with the "__h5scripting" attribute set to True are accepted

    returns : a dictionary such as {"Data1": DataObject1, "Data2": DataObject2, ...}
        where the names are the h5 dataset names.
    """
    pass

    
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
        import ast
        
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

        # Return a wrapped version of the function that executes
        # in a restricted environment to ensure it doesn't have
        # dependencies on global or nonlocal variables:
        return _create_sandboxed_callable(self.filename, function_name, function_source, args, kwargs)


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
    decorator = attached_function(filename, name, docstring, groupname, args, kwargs)
    sandboxed_fuction = attached_function(function)
    return sandboxed_function
 
 
def _create_sandboxed_callable(filename, function_name, function_source, args, kwargs):
    """
    Creates a callable function from the function name and source code.

    This callable executes in an empty namespace, and so does not have
    access to global and local variables in the calling scope.

    When called, it automatically receives 'filename' as its first
    argument, args and kwargs as its arguments and keyword arguments.
    
    It can accept further keyword arguments as well when called, but 
    """

    import functools
    # Exec the function definition to get the function object:
    sandbox_namespace = {}
    exec_in_namespace(function_source, sandbox_namespace)
    function = sandbox_namespace[function_name]

    # Define a wrapped version of the function that always executes
    # in an empty namespace, so as to ensure it is self contained:
    @functools.wraps(function)
    def sandboxed_function(**passed_kwargs):
        # Names mangled to reduce risk of colliding with the function
        # attempting to access global variables (which it shouldn't be doing):
        sandbox_kwargs = {}
        sandbox_kwargs.update(kwargs)
        sandbox_kwargs.update(passed_kwargs)
        sandbox_namespace = {'__h5s_filename': filename,
                             '__h5s_function': function,
                             '__h5s_args': args,
                             '__h5s_kwargs': sandbox_kwargs}
        exc_line = '__h5s_result = __h5s_function(__h5s_filename, *__h5s_args, **__h5s_kwargs)'
        exec_in_namespace(exc_line, sandbox_namespace)
        result = sandbox_namespace['__h5s_result']
        return result

    return sandboxed_function

    
def _extract_saved_function(filename, dataset):
    """
    Helper function to extract the saved function from an already open
    h5 file
    """

    function_source = dataset.value
    function_name = dataset.attrs['function_name']
    function_docstring = dataset.attrs['function_docstring']
    function_signature = dataset.attrs['function_signature']
    function_args = ast.literal_eval(dataset.attrs['function_args'])
    function_kwargs = ast.literal_eval(dataset.attrs['function_kwargs'])

    saved_function = {
        "function" : _create_sandboxed_callable(filename, function_name, function_source, function_args, function_kwargs),
        "function_docstring": function_docstring,
        "function_signature": function_signature,
        "function_source": function_source,
        "function_args": function_args,
        "function_kwargs": function_kwargs}

    return saved_function


def get_saved_function(filename, name, groupname='saved_functions'):
    """
    Retrieves a previously saved function from the h5 file.

    The function is returned as a callable that will run in an
    empty namespace with no access to global or local variables
    in the calling scope.

    filename : h5 file to use

    name : the name of the dataset to which the function is saved.
        if this was not set when saving the function with
        attach_function(), then this is the name of the function
        itself.

    groupname : the group in the h5 file to which the function is saved.
        Defaults to 'saved_functions'
        
    returns {
        "function": Function, 
        "function_docstring": FunctionDocString,
        "function_signature": FunctionSignature,
        "function_source" : FunctionSource}
    """

    with h5py.File(filename, "r") as f:
        group = f[groupname]
        dataset = group[name]
        saved_function = _extract_saved_function(filename, dataset)
    
    return saved_function


def get_all_saved_functions(filename, groupname='saved_functions'):
    """
    Retruns all the saved functions in the group deined by groupname as 
    a dictionary of the form:
    
    {"function_name": {
        "function": Function, 
        "function_docstring": FunctionDocString,
        "function_signature": FunctionSignature,
        "function_source" : FunctionSource}, ...}
    
    This assumes that all of the datasets in groupname are saved functions.
    """
    with h5py.File(filename, "r") as f:
        group = f[groupname]
        keys = group.keys()
        
        saved_functions = {}
        for key in keys:
            dataset = group[key]
            saved_function = _extract_saved_function(filename, dataset)
            saved_functions[key] = saved_function

    return saved_functions
