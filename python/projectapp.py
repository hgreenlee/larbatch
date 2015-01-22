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

# Standard imports

import sys, os

# Import project.py as a module.

import project

# Import Tkinter GUI stuff

import Tkinter as tk
#import ttk
import tkFileDialog
import tkMessageBox
from projectview import ProjectView

# Main window class for this gui application.

class ProjectApp(tk.Frame):

    # Constructor.

    def __init__(self, parent=None):

        # Root window.

        if parent == None:
            self.root = tk.Tk()
        else:
            self.root = parent
        self.root.title('project')
        self.root.protocol('WM_DELETE_WINDOW', self.close)

        # Register our outermost frame in the parent window.

        tk.Frame.__init__(self, self.root)
        self.pack(expand=1, fill=tk.BOTH)

        # Make widgets that belong to this app.

        self.make_widgets()

        # Current project (menu settable).

        self.current_project = tk.StringVar()

        # Known projects.
        self.projects = []

        # Process command line arguments.

        for n in range(1, len(sys.argv)):
            xmlpath = sys.argv[n]
            self.open(xmlpath, choose=False)
        if len(self.projects) > 0:
            self.choose_project(self.projects[0][0])

    # Make widgets.  Make and register all widgets in the application window.

    def make_widgets(self):

        # Make menu bar.

        self.make_menubar()

        # Add project view widget.

        self.project_view = ProjectView(self)
        self.project_view.pack(side=tk.BOTTOM, expand=1, fill=tk.BOTH)

    # Make a menubar widget.

    def make_menubar(self):

        # Put menu in its own frame.

        self.menubar = tk.Frame(self)
        self.menubar.pack(side=tk.TOP, anchor=tk.NW)

        # File menu.

        mbutton = tk.Menubutton(self.menubar, text='File')
        mbutton.pack(side=tk.LEFT)
        file_menu = tk.Menu(mbutton)
        file_menu.add_command(label='Open Project', command=self.open)
        file_menu.add_command(label='Quit', command=self.close)
        mbutton['menu'] = file_menu

        # View menu.

        mbutton = tk.Menubutton(self.menubar, text='View')
        mbutton.pack(side=tk.LEFT)
        view_menu = tk.Menu(mbutton)
        view_menu.add_command(label='XML', command=self.xml_view)
        view_menu.add_command(label='Project status', command=self.project_status_view)
        mbutton['menu'] = view_menu

        # Project menu.

        mbutton = tk.Menubutton(self.menubar, text='Project')
        mbutton.pack(side=tk.LEFT)
        self.project_menu = tk.Menu(mbutton)
        self.project_menu.add_command(label='Next Project', command=self.next_project,
                                      accelerator='PgUp')
        self.project_menu.add_command(label='Previous Project', command=self.previous_project,
                                      accelerator='PgDn')
        self.project_menu.add_separator()
        mbutton['menu'] = self.project_menu

        # Add key bindings.

        self.project_menu.bind_all('<KeyPress-KP_Next>', self.next_project_handler)
        self.project_menu.bind_all('<KeyPress-Next>', self.next_project_handler)
        self.project_menu.bind_all('<KeyPress-KP_Prior>', self.previous_project_handler)
        self.project_menu.bind_all('<KeyPress-Prior>', self.previous_project_handler)

        return

    # Open project.

    def open(self, xml_path=None, choose=True):
        if xml_path == None:
            types = (('XML files', '*.xml'),
                     ('All files', '*'))
            d = tkFileDialog.Open(filetypes=types, parent=self.root)
            xml_path = d.show()

        # Parse xml into ProjectDef object, and from that extract project name.
        # This step can raise an exception for several reasons.  In that case, 
        # display a message and return without opening the file.

        try:
            project_def = project.ProjectDef(project.get_project(xml_path, ''), False)
        except:
            message = 'Error opening %s' % xml_path
            tkMessageBox.showwarning('', message)
            return
            
        project_name = project_def.name

        # See if this project is already in the list of open projects.
        # If so, just choose this project, but don't open it again.

        for project_tuple in self.projects:
            if project_name == project_tuple[0]:
                if choose:
                    self.choose_project(project_name)
                return

        # Add this project to the list of known projects.

        self.projects.append((project_name, xml_path, project_def))

        # Update project menu.

        callback = tk._setit(self.current_project, project_name, callback=self.choose_project)
        self.project_menu.add_command(label=project_name, command=callback)

        # Choose just-opened project.

        if choose:
            self.choose_project(project_name)

    # Choose already-opened project.

    def choose_project(self, value):
        self.current_project = value
        for project_tuple in self.projects:
            project_name = project_tuple[0]
            xml_path = project_tuple[1]
            project_def = project_tuple[2]
            if project_name == value:

                # Update project widget with name of xml file.

                self.project_view.set_project(project_name, xml_path, project_def)
                return
    
        # It is an error if we fall out of the loop.

        raise 'No project: %s' % value

    # Key event handlers.

    def next_project_handler(self, event):
        self.next_project()
    def previous_project_handler(self, event):
        self.previous_project()
        
    # Select next project.
    def next_project(self):

        # Don't do anything if the project list is empty.

        if len(self.projects) == 0:
            return

        # Cycle through list of known projects.

        found = False
        for project_tuple in self.projects:
            project_name = project_tuple[0]
            if found:
                self.choose_project(project_name)
                return
            if project_name == self.current_project:
                found = True

        # Choose first project if we fell out of the loop.

        self.choose_project(self.projects[0][0])

    # Select previous project.

    def previous_project(self):

        # Don't do anything if the project list is empty.

        if len(self.projects) == 0:
            return

        # Cycle through list of known projects.

        previous_project_name = self.projects[-1][0]
        for project_tuple in self.projects:
            project_name = project_tuple[0]
            if project_name == self.current_project:
                self.choose_project(previous_project_name)
                return
            previous_project_name = project_name

        # Choose the last project if we fell out of the loop.

        self.choose_project(self.projects[-1][0])

    # Select XML view.

    def xml_view(self):
        self.project_view.xml_view()

    # Select project status view.

    def project_status_view(self):
        self.project_view.project_status_view()

    # Close window.

    def close(self):
        self.root.destroy()
