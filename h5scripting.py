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
import h5py._hl.dataset 
import numpy

# -----------------------------------------------------------------------------
#
# START Override h5py objects
#
# -----------------------------------------------------------------------------

class HLObject(object):
    """
    This adds functionality to every class below, as they are all ansesters
    of the h5py HLObject class.  Maybe there is a better way of doing this
    where I override HLObject and don't have to specifiy anything, but I didn't
    see a way to do this.
    
    Here we allow the easy attachment of docstrings and labeling of
    __h5scripting__ types
    """

    @property
    def docstring(self):
        return self.attrs['__h5scripting__doc__']

    @docstring.setter
    def docstring(self, value):
        self.attrs['__h5scripting__doc__'] = value

    @property
    def h5scripting_id(self):
        return self.attrs['__h5scripting__']

    @h5scripting_id.setter
    def h5scripting_id(self, value):
        self.attrs['__h5scripting__'] = value

    def _check_h5scripting_id(self, type_string):
        valid = ('__h5scripting__doc__' in self.attrs and
                 '__h5scripting__' in self.attrs and
                 self.attrs['__h5scripting__'] == type_string)
        return valid

    def _valid_h5scripting_object(self, type_string, throw_error=False):
        """
        Looks for the __h5scripting__ attribute in h5scripting_obj and if present
        verifies that it matches type_string
        
        h5scripting_obj generally will be an h5py group, file, or dataset
        
        throw_error : if true, will throw a type error
        """
    
        # Don't do anything if _ErrorCheck is off
        if not self._ErrorCheck:
            return True

        valid = self._check_h5scripting_id(type_string)
        
        if throw_error and not valid:
            raise TypeError('not a valid h5scripting %s: __h5scripting__  or __h5scripting__doc__ attribute missing or invalid'%type_string)             
    
        return valid


class Dataset(HLObject, h5py.Dataset):
    def __init__(self, bind, h5scripting_id = "dataset", ErrorCheck=True):
        super().__init__(bind)
        
        # This dataset should already have been created and be fully valid
        # verify that this is the case
        self._ErrorCheck = ErrorCheck
        self._valid_h5scripting_object(h5scripting_id, throw_error = True)

class GroupMixins():
    def create_group(self, name, 
                 docstring = "", h5scripting_id = "group"):
        """ Create and return a new h5scripting managed subgroup.

        Name may be absolute or relative.  Fails if the target name already
        exists.

        Accepts docstring = "", h5scripting_id = "group"
        """

        name, lcpl = self._e(name, lcpl=True)
        gid = h5py.h5g.create(self.id, name, lcpl=lcpl)
        grp = Group(gid, ErrorCheck=False)

        # if possible tag the group
        grp.h5scripting_id = h5scripting_id
            
        if "__h5scripting__doc__" not in grp.attrs or docstring != '':
            grp.docstring = docstring
        
        return grp

    def create_dataset(self, name, shape=None, dtype=None, data=None, 
                       docstring = "", h5scripting_id = "dataset", **kwds):
        """ Create a new h5scripting managed HDF5 dataset

        name
            Name of the dataset (absolute or relative).  Provide None to make
            an anonymous dataset.
        shape
            Dataset shape.  Use "()" for scalar datasets.  Required if "data"
            isn't provided.
        dtype
            Numpy dtype or string.  If omitted, dtype('f') will be used.
            Required if "data" isn't provided; otherwise, overrides data
            array's dtype.
        data
            Provide data to initialize the dataset.  If used, you can omit
            shape and dtype arguments.

        Keyword-only arguments:

        chunks
            (Tuple) Chunk shape, or True to enable auto-chunking.
        maxshape
            (Tuple) Make the dataset resizable up to this shape.  Use None for
            axes you want to be unlimited.
        compression
            (String) Compression strategy.  Legal values are 'gzip', 'szip',
            'lzf'.  Can also use an integer in range(10) indicating gzip.
        compression_opts
            Compression settings.  This is an integer for gzip, 2-tuple for
            szip, etc.
        scaleoffset
            (Integer) Enable scale/offset filter for (usually) lossy
            compression of integer or floating-point data. For integer
            data, the value of scaleoffset is the number of bits to
            retain (pass 0 to let HDF5 determine the minimum number of
            bits necessary for lossless compression). For floating point
            data, scaleoffset is the number of digits after the decimal
            place to retain; stored values thus have absolute error
            less than 0.5*10**(-scaleoffset).
        shuffle
            (T/F) Enable shuffle filter.
        fletcher32
            (T/F) Enable fletcher32 error detection. Not permitted in
            conjunction with the scale/offset filter.
        fillvalue
            (Scalar) Use this value for uninitialized parts of the dataset.
        track_times
            (T/F) Enable dataset creation timestamps.

        Accepts docstring = "", h5scripting_id = "group"
        """
         
        dsid = h5py._hl.dataset.make_new_dset(self, shape, dtype, data, **kwds)
        dset = Dataset(dsid, ErrorCheck=False)
        if name is not None:
            self[name] = dset

        # if possible tag the group
        dset.h5scripting_id = h5scripting_id
            
        if "__h5scripting__doc__" not in dset.attrs or docstring != '':
            dset.docstring = docstring
        
        return dset

    def require_dataset(self, name, shape, dtype, exact=False, 
                        docstring = "", h5scripting_id = "group", **kwds):
        """ Open a dataset, creating it if it doesn't exist.

        If keyword "exact" is False (default), an existing dataset must have
        the same shape and a conversion-compatible dtype to be returned.  If
        True, the shape and dtype must match exactly.

        Other dataset keywords (see create_dataset) may be provided, but are
        only used if a new dataset is to be created.

        Raises TypeError if an incompatible object already exists, or if the
        shape or dtype don't match according to the above rules.

        Accepts docstring = "", h5scripting_id = "group"
        """

        if not name in self:
            return self.create_dataset(name, 
                                       docstring = docstring, 
                                       h5scripting_id = h5scripting_id,
                                       *(shape, dtype), **kwds)

        dset = self[name]
        if not isinstance(dset, Dataset):
            raise TypeError("Incompatible object (%s) already exists" % dset.__class__.__name__)

        if not shape == dset.shape:
            raise TypeError("Shapes do not match (existing %s vs new %s)" % (dset.shape, shape))

        if exact:
            if not dtype == dset.dtype:
                raise TypeError("Datatypes do not exactly match (existing %s vs new %s)" % (dset.dtype, dtype))
        elif not numpy.can_cast(dtype, dset.dtype):
            raise TypeError("Datatypes cannot be safely cast (existing %s vs new %s)" % (dset.dtype, dtype))

        return dset

    def require_group(self, name, docstring = "", h5scripting_id = "group"):
        """ Return a group, creating it if it doesn't exist.

        TypeError is raised if something with that name already exists that
        isn't a group.
        
        Accepts docstring = "", h5scripting_id = "group"
        """

        if not name in self:
            return self.create_group(name, 
                                     docstring = docstring, 
                                     h5scripting_id = h5scripting_id)
        grp = self[name]
        grp.docstring = docstring
        grp.h5scripting_id = h5scripting_id
        
        if not isinstance(grp, Group):
            raise TypeError("Incompatible object (%s) already exists" % grp.__class__.__name__)
        return grp

    def getitem(self, name, h5scripting_id = None):
        """ Open an object in the file """
        if isinstance(name, h5py.h5r.Reference):
            oid = h5py.h5r.dereference(name, self.id)
            if oid is None:
                raise ValueError("Invalid HDF5 object reference")
        else:
            oid = h5py.h5o.open(self.id, self._e(name), lapl=self._lapl)

        otype = h5py.h5i.get_type(oid)
        if h5scripting_id is None:
            if otype == h5py.h5i.GROUP:
                return Group(oid, ErrorCheck = self._ErrorCheck)
            elif otype == h5py.h5i.DATASET:
                return Dataset(oid, ErrorCheck = self._ErrorCheck)
            elif otype == h5py.h5i.DATATYPE:
                return h5py.datatype.Datatype(oid)
            else:
                raise TypeError("Unknown object type")
        else: # New case to allow different h5scripting_id tags
            if otype == h5py.h5i.GROUP:
                return Group(oid, ErrorCheck = self._ErrorCheck, h5scripting_id = h5scripting_id)
            elif otype == h5py.h5i.DATASET:
                return Dataset(oid, ErrorCheck = self._ErrorCheck, h5scripting_id = h5scripting_id)
            elif otype == h5py.h5i.DATATYPE:
                return h5py.datatype.Datatype(oid)
            else:
                raise TypeError("Unknown object type")
            
        

    def __getitem__(self, name):
        """ Open an object in the file """
        # Move this code into getitem to allow desired kw argument to be passed
        return self.getitem(name)

class Group(GroupMixins, HLObject, h5py.Group):
    def __init__(self, bind, h5scripting_id = "group", ErrorCheck=True):
        super().__init__(bind)
        
        # This group should already have been created and be fully valid
        # verify that this is the case
        self._ErrorCheck = ErrorCheck
        self._valid_h5scripting_object(h5scripting_id, throw_error = True)

# The operation of this relies on the resolution order going from
# HLObject to h5py.File to Group before h5py.Group (which is an ancestor of 
# h5py.File)
class File(GroupMixins, HLObject, h5py.File):
    def __init__(self, name, mode=None, 
                 docstring = "", h5scripting_id = "file", ErrorCheck = True, 
                 *args, **kwargs):
        super().__init__(name, mode=mode, *args, **kwargs)

        self._ErrorCheck = ErrorCheck
        
        # When in read only mode, verify that this is a h5scripting manged file
        # when in all writing modes, make it a h5scripting managed file
        if mode in ("r", "r+"):
            self._valid_h5scripting_object(h5scripting_id, throw_error = True)
        else:
            # Disable error checking
            self._ErrorCheck = False
            
            self.h5scripting_id = h5scripting_id
            
            # If passed an interesting string, or if doc is absent
            if docstring != '' or '__h5scripting__doc__' not in self.attrs:
                self.docstring = docstring
                
            # Enable Error Checking
            self._ErrorCheck = True

    @property
    def attrs(self):
        """ Attributes attached to this object """
        # hdf5 complains that a file identifier is an invalid location for an
        # attribute. Instead of self, pass the root group to AttributeManager:
        from h5py._hl import attrs
        
        # For archane reasons, this code cannot run unless error checking is
        # turned off.
        ErrorCheck = self._ErrorCheck
        self._ErrorCheck = False
        ret = attrs.AttributeManager(self['/'])
        self._ErrorCheck = ErrorCheck
        return ret
               
# -----------------------------------------------------------------------------
#
# END Override h5py objects
#
# -----------------------------------------------------------------------------

def exec_in_namespace(code, namespace):
    if sys.version < '3':
        exec("""exec code in namespace""")
    else:
        if isinstance(__builtins__, dict):
            exec_func = __builtins__['exec']
        else:
            exec_func = getattr(__builtins__, 'exec')
        exec_func(code, namespace) 

    
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
                "\n----- ADDITIONAL DOCSTRING -----\n" +
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
        if not (isinstance(args, list) or isinstance(args, tuple)):
            raise TypeError('args must be a list or a tuple')
        function_args = repr(args)
        try:
            assert ast.literal_eval(function_args) == args
        except Exception:
            raise ValueError('Argument list can contain only Python literals')
        
        if self.kwargs is None:
            kwargs = {}
        else:
            kwargs = self.kwargs
        if not isinstance(kwargs, dict):
            raise TypeError('kwargs must be a dictionary')
        function_kwargs = repr(kwargs)
        try:
            assert ast.literal_eval(function_kwargs) == kwargs
        except Exception:
            raise TypeError('Keyword argument list can contain only Python literals')
            
        argspec = inspect.getargspec(function)
        function_signature = function_name + inspect.formatargspec(*argspec)
        try:
            function_source = inspect.getsource(function)
        except Exception:
            raise TypeError('Could not get source code of %s %s. '%(type(function).__name__, repr(function)) + 
                            'Only ordinary Python functions defined in Python source code can be saved.')
            
        function_lines = function_source.splitlines()
        indentation = min(len(line) - len(line.lstrip(' ')) for line in function_lines)
        # Remove this decorator from the source, if present:
        if function_lines[0][indentation:].startswith('@'):
            del function_lines[0]
        # Remove initial indentation from the source:
        function_source = '\n'.join(line[indentation:] for line in function_lines)

        with File(self.filename) as f:
            group = f.require_group(self.groupname,  h5scripting_id = 'functions_group')
            try:
                del group[name]
            except KeyError:
                pass
            dataset = group.create_dataset(name, data=function_source,
                                           docstring = function_docstring,
                                           h5scripting_id = 'function')
            dataset.attrs['__h5scripting__function_name__'] = function_name
            dataset.attrs['__h5scripting__function_signature__'] = function_signature
            dataset.attrs['__h5scripting__function_args__'] = function_args
            dataset.attrs['__h5scripting__function_kwargs__'] = function_kwargs
            
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
        function_docstring = dataset.docstring
        function_name = dataset.attrs['__h5scripting__function_name__']
        function_signature = dataset.attrs['__h5scripting__function_signature__']
        function_args = ast.literal_eval(dataset.attrs['__h5scripting__function_args__'])
        function_kwargs = ast.literal_eval(dataset.attrs['__h5scripting__function_kwargs__'])
        
        # Exec the function definition to get the function object:
        sandbox_namespace = {}
        exec_in_namespace(function_source, sandbox_namespace)
        function = sandbox_namespace[function_name]
    
        self._function = function
        self.name = dataset.name
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
                '    name=%s\n'%self.name +
                '    function_name=%s\n'%self.function_name + 
                '    function_source=%s\n'%function_source +
                '    function_docstring=%s\n'%function_docstring + 
                '    function_args=%s\n'%function_args + 
                '    function_kwargs=%s\n'%function_kwargs + 
                '    h5_filename=%s>'%self.h5_filename)
                
    def do_all(self):
        """
        evaluates the function and also plots relevant data
        """
        
        sep = "-"
        for x in range(80): sep += "-"
        
        print(sep)
        print(sep)
        print(self.name)
        print(sep)
        print(sep)
        print(self.function_docstring)
        print(sep)
        print(self.function_source)
        print(sep)
        self()
        print(sep + "\n")

        
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

    with File(filename, "r") as f:
        grp = f.getitem(groupname, h5scripting_id="functions_group")
        dataset = grp.getitem(name, h5scripting_id="function")
        saved_function = SavedFunction(dataset)
    
    return saved_function


def get_all_saved_functions(filename, groupname='saved_functions'):
    """
    returns all the saved functions in the group deined by groupname as 
    a list of the form:
    
    [saved_function, ]
    
    This assumes that all of the datasets in groupname are saved functions.
    """
    
    saved_functions = []
    with File(filename, "r",) as f:
        grp = f.getitem(groupname, h5scripting_id="functions_group")
        
        grp._ErrorCheck = False
        for dataset in grp.values():
            if dataset._check_h5scripting_id("function"):
                saved_functions += [SavedFunction(dataset),]

    return saved_functions

def list_all_saved_functions(filename, groupname='saved_functions'):
    """
    returns all the saved functions in the group deined by groupname as 
    a list of the form:
    
    [saved_function, ]
    
    This assumes that all of the datasets in groupname are saved functions.
    """
    
    saved_functions = get_all_saved_functions(filename, groupname=groupname)

    datalist = []
    for function in saved_functions:
        docstring = ""

        # Build string for data in this group
        docstring += "FUNCTION DATASET: %s\n\n"%function.name
        docstring += "FUNCTION NAME: %s\n\n"%function.function_name

        docstring += "FUNCTION DOCSTRING:%s\n"%function.function_docstring
        
                    
        docstring += "-------------------------------------------------"

        datalist += [docstring,]
        

    return datalist

def get_all_data(filename, groupname):
    """
    Gets data from an existing h5 file.

    filename : h5 file to use

    groupname : group to use
    
    only datasets with the "__h5scripting__" attribute set to 'dataset' are accepted

    returns : a dictionary such as {
        "Data1": DataObject1,
        "Data2": DataObject2, 
        ...}
        where the names are the h5 dataset names.
    """

    h5data = {}
    with File(filename, 'r') as f:
        grp = f[groupname]

        grp._ErrorCheck = False
        for dataset in grp.values():
            if dataset._check_h5scripting_id("dataset"):
                key = dataset.name
                key = key.split("/")[-1]
                h5data[key] = dataset.value

    return h5data

def list_all_saved_data(filename, groupname=None):
    """
    returns the paths of all saved data, the metadata, and the names of the
    data, and the basic information (shape, type) about the data
    
    This function is designed for interactive operation when you have an h5
    file and want to be reminded what data to use to make a figure    
    
    [string1, ]
    
    each string looks like
    
        group name
        group docstring
        
        data1 : shape, type
            data1 docstring
        data2 : shape, type
            data2 docstring
        ....
        
        docstring
    
    This shows the importance of writing a good docstring to inform the user
    what each of the data really are.    
    
    This assumes that all of the datasets in groupname are saved functions.
    
    This function is mostly designed for intractive use to inspect the data in
    the file to make it more simple to write functions.
    """

    class cls(object):
        """
        provides a callable for grp.visititems to call
        """
        def __init__(self):
            self.datalist = []
        
        def __call__(self, name, obj):
            """
            obj will either be a file, group, or dataset object
            """
            
            if obj._check_h5scripting_id("group"):
                docstring = ""
    
                # Build string for data in this group
                docstring += "GROUP: %s\n\n"%name
    
                docstring += "GROUP DOCSTRING:%s\n"%(obj.attrs['__h5scripting__doc__'])
                
                # iterate over datasets in group
                for dataset in obj.values():
                    if dataset._check_h5scripting_id("dataset"):
                        docstring += "DATASET %s: %s, %s\n"%(
                            dataset.name.split("/")[-1],
                            str(dataset.value.shape),
                            str(dataset.value.dtype))
                        docstring += "\t%s\n"%dataset.attrs['__h5scripting__doc__']
                            
                docstring += "-------------------------------------------------"
    
                self.datalist += [docstring,]

    func = cls()

    with File(filename, "r") as f:
        f._ErrorCheck = False
        grp = f if groupname is None else f[groupname]
        grp.visititems(func)

    return func.datalist
        
    
