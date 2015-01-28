#! /usr/bin/env python
######################################################################
#
# Name: textwindow.py
#
# Purpose: Python class for displaying arbitrary text in a scrolling
#          top level window.
#
# Created: 28-Jan-2015  Herbert Greenlee
#
######################################################################

# Import GUI stuff

import Tkinter as tk

# Project widget class

class TextWindow():

    # Constructor.

    def __init__(self):

        # Make a Toplevel widget.

        self.window = tk.Toplevel()
        self.window.rowconfigure(0, weight=1)
        self.window.columnconfigure(0, weight=1)

        # Add an empty text widget.

        self.text = tk.Text(self.window, height=24, width=80, wrap=tk.NONE)
        self.text.grid(row=0, column=0, sticky=tk.N+tk.E+tk.W+tk.S)

        # Make scroll bars, but don't grid them yet.

        self.vbar = tk.Scrollbar(self.window, orient=tk.VERTICAL, command=self.text.yview)
        self.text['yscrollcommand'] = self.vbar.set
        self.vbar_visible = 0

        self.hbar = tk.Scrollbar(self.window, orient=tk.HORIZONTAL, command=self.text.xview)
        self.text['xscrollcommand'] = self.hbar.set
        self.hbar_visible = 0

        self.check_scroll()
 
	# Set event bindings (check scrollbar visibility when window size 
        # changes).

	self.text.bind('<Configure>', self.check_scroll)

    # See if we need to enable or disable either scrollbar.

    def check_scroll(self, event=None):

        # Check vertical scroll bar.

	yv = self.text.yview()
	if not self.vbar_visible and (yv[0] != 0.0 or yv[1] != 1.0):
	    self.vbar_visible = 1
	    self.vbar.grid(row=0, column=1, sticky=tk.N+tk.S)
	elif self.vbar_visible and yv[0] == 0.0 and yv[1] == 1.0:
	    self.vbar_visible = 0
	    self.vbar.grid_forget()

        # Check horizontal scroll bar.

	xv = self.text.xview()
	if not self.hbar_visible and (xv[0] != 0.0 or xv[1] != 1.0):
	    self.hbar_visible = 1
	    self.hbar.grid(row=1, column=0, sticky=tk.E+tk.W)
	elif self.hbar_visible and xv[0] == 0.0 and xv[1] == 1.0:
	    self.hbar_visible = 0
	    self.hbar.grid_forget()

    # Insert text.

    def insert(self, pos, text):
        self.text.insert(pos, text)

