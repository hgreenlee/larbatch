#! /usr/bin/env python
######################################################################
#
# Name: projectgui.py
#
# Purpose: Script for invoking gui interface to project.py
#
# Created: 16-Jan-2015  Herbert Greenlee
#
# Usage:
#
# projectgui.py
#
######################################################################

from project_gui_modules.projectapp import ProjectApp

w = ProjectApp()
w.mainloop()
