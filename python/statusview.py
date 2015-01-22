#! /usr/bin/env python
######################################################################
#
# Name: statusview.py
#
# Purpose: Python class StatusView is a widget for displaying 
#          project status information.
#
# Created: 22-Jan-2015  Herbert Greenlee
#
######################################################################

# Import GUI stuff

import Tkinter as tk

# Project widget class

class ProjectStatusView(tk.Frame):

    # Constructor.

    def __init__(self, parent):

        self.root = parent

        # Register our outermost frame in the parent window.

        tk.Frame.__init__(self, self.root)
        self.pack(expand=1, fill=tk.BOTH)
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

        # Make widgets that belong to this widget.

        self.make_widgets()

    # Make widgets.

    def make_widgets(self):

        # Add a test label.

        self.label = tk.Label(self, relief=tk.SUNKEN, text='Project status')
        self.label.grid(row=0, column=0, sticky=tk.N+tk.E+tk.W+tk.S)
