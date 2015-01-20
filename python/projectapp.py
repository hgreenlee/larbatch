#! /usr/bin/env python
######################################################################
#
# Name: projectapp.py
#
# Purpose: Python class ProjectApp for project.py gui interface.
#
# Created: 16-Jan-2015  Herbert Greenlee
#
######################################################################

# Import GUI stuff

from Tkinter import *
import tkFileDialog

# Main window class for this gui application.

class ProjectApp(Frame):

    # Constructor.

    def __init__(self, parent=None):

        # Root window.

        if parent == None:
            self.root = Tk()
        else:
            self.root = parent
        self.root.protocol('WM_DELETE_WINDOW', self.close)

        self.make_widgets()
       

    # Make widgets.  Make and register all widgets in the application window.

    def make_widgets(self):

        # Register our outermost frame in the parent window.

        Frame.__init__(self, self.root)
        self.grid(sticky=N+E+S+W)
        self.winfo_toplevel().rowconfigure(0, weight=1)
        self.winfo_toplevel().columnconfigure(0, weight=1)

        # Make menu bar.

        self.menu = self.make_menubar()
        self.menu.grid(row=0, column=0, sticky=N+W)

        # Add a button (testing).

        self.button = Button(self, text='Quit', command=self.close)
        self.button.grid(row=1, column=0, sticky=N+E+S+W)
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)

    # Make a menubar widget.

    def make_menubar(self):

        # Put menu in its own frame.

        menubar = Frame(self)

        # File menu.

        mbutton = Menubutton(menubar, text='File')
        mbutton.grid(row=0, column=0, sticky=W)
        file_menu = Menu(mbutton)
        file_menu.add_command(label='Open Project', command=self.open)
        file_menu.add_command(label='Quit', command=self.close)
        mbutton['menu'] = file_menu
        mbutton = Menubutton(menubar, text='Project')
        mbutton.grid(row=0, column=1, sticky=W)
        mbutton = Menubutton(menubar, text='Stage')
        mbutton.grid(row=0, column=2, stick=W)
        return menubar

    # Open project.
    def open(self):
        types = (('XML files', '*.xml'),
                 ('All files', '*'))
        d = tkFileDialog.Open(filetypes=types, parent=self.root)
        file = d.show()
        print file

    # Close window.

    def close(self):
        self.root.destroy()
