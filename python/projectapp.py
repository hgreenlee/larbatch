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
       

    # Make widgets.

    def make_widgets(self):
        Frame.__init__(self, self.root)
        self.pack(expand=1, fill=BOTH)

    # Close window.

    def close(self):
        self.root.destroy()
