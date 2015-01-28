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

import sys, os, subprocess

# Import project.py as a module.

import project
from batchstatus import BatchStatus

# Import Tkinter GUI stuff

import Tkinter as tk
#import ttk
import tkFileDialog
import tkMessageBox
import tkFont
from projectview import ProjectView

ticket_ok = False

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

        # Current project and stage (menu settable).

        self.current_project_name = ''
        self.current_project_def = None
        self.current_stage_name = ''
        self.current_stage_def = None

        # Known projects (3-tuple: project_name, xml_path, project_def)

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

        mbutton = tk.Menubutton(self.menubar, text='File', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        file_menu = tk.Menu(mbutton)
        file_menu.add_command(label='Open Project', command=self.open)
        file_menu.add_command(label='Quit', command=self.close)
        mbutton['menu'] = file_menu

        # View menu.

        mbutton = tk.Menubutton(self.menubar, text='View', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        view_menu = tk.Menu(mbutton)
        view_menu.add_command(label='XML', command=self.xml_view)
        view_menu.add_command(label='Project status', command=self.project_status_view)
        mbutton['menu'] = view_menu

        # Project menu.

        mbutton = tk.Menubutton(self.menubar, text='Project', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        self.project_menu = tk.Menu(mbutton)
        self.project_menu.add_command(label='Next Project', command=self.next_project,
                                      accelerator='PgUp')
        self.project_menu.add_command(label='Previous Project', command=self.previous_project,
                                      accelerator='PgDn')
        self.project_menu.add_separator()
        mbutton['menu'] = self.project_menu

        # Add project menu key bindings.

        self.project_menu.bind_all('<KeyPress-KP_Next>', self.next_project_handler)
        self.project_menu.bind_all('<KeyPress-Next>', self.next_project_handler)
        self.project_menu.bind_all('<KeyPress-KP_Prior>', self.previous_project_handler)
        self.project_menu.bind_all('<KeyPress-Prior>', self.previous_project_handler)

        # Stage menu.

        mbutton = tk.Menubutton(self.menubar, text='Stage', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        self.stage_menu = tk.Menu(mbutton)
        self.stage_menu.add_command(label='Next Stage', command=self.next_stage,
                                    accelerator='Up')
        self.stage_menu.add_command(label='Previous Stage', command=self.previous_stage,
                                    accelerator='Down')
        self.stage_menu.add_separator()
        mbutton['menu'] = self.stage_menu

        # Add stage menu key bindings.

        self.stage_menu.bind_all('<KeyPress-KP_Up>', self.previous_stage_handler)
        self.stage_menu.bind_all('<KeyPress-Up>', self.previous_stage_handler)
        self.stage_menu.bind_all('<KeyPress-KP_Down>', self.next_stage_handler)
        self.stage_menu.bind_all('<KeyPress-Down>', self.next_stage_handler)

        # Output menu.

        mbutton = tk.Menubutton(self.menubar, text='Output', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        self.output_menu = tk.Menu(mbutton)
        self.output_menu.add_command(label='Check', command=self.check)
        self.output_menu.add_command(label='Checkana', command=self.checkana)
        self.output_menu.add_command(label='Fetchlog', command=self.fetchlog)
        self.output_menu.add_command(label='Clean', command=self.clean)
        mbutton['menu'] = self.output_menu

        # Batch menu.

        mbutton = tk.Menubutton(self.menubar, text='Batch', font=tkFont.Font(size=12))
        mbutton.pack(side=tk.LEFT)
        self.batch_menu = tk.Menu(mbutton)
        self.batch_menu.add_command(label='Submit', command=self.submit)
        self.batch_menu.add_command(label='Makeup', command=self.makeup)
        self.batch_menu.add_command(label='Update', command=self.update_jobs)
        mbutton['menu'] = self.batch_menu
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
            e = sys.exc_info()
            message = 'Error opening %s\n%s' % (xml_path, e[1])
            tkMessageBox.showerror('', message)
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

        callback = tk._setit(tk.StringVar(), project_name, callback=self.choose_project)
        self.project_menu.add_command(label=project_name, command=callback)

        # Choose just-opened project.

        if choose:
            self.choose_project(project_name)

    # Choose already-opened project.

    def choose_project(self, value):
        self.current_project_name = ''
        self.current_project_def = None
        self.current_stage_name = ''
        self.current_stage_def = None
        self.project_view.highlight_stage(self.current_stage_name)
        for project_tuple in self.projects:
            project_name = project_tuple[0]
            xml_path = project_tuple[1]
            project_def = project_tuple[2]
            if project_name == value:
                self.current_project_name = value
                self.current_project_def = project_def

                # Update project view widget.

                self.project_view.set_project(project_name, xml_path, project_def)

                # Update stage menu.

                self.stage_menu.delete(3, tk.END)
                self.stage_menu.add_separator()
                for stage in project_def.stages:
                    callback = tk._setit(tk.StringVar(), stage.name, callback=self.choose_stage)
                    self.stage_menu.add_command(label=stage.name, command=callback)

                return
    
        # It is an error if we fall out of the loop.

        raise 'No project: %s' % value

    # Choose stage.

    def choose_stage(self, value):
        if self.current_project_def != None:
            for stage in self.current_project_def.stages:
                if stage.name == value:
                    self.current_stage_name = value
                    self.current_stage_def = stage
                    self.project_view.highlight_stage(self.current_stage_name)

    # Key event handlers.

    def next_project_handler(self, event):
        self.next_project()
    def previous_project_handler(self, event):
        self.previous_project()
    def next_stage_handler(self, event):
        self.next_stage()
    def previous_stage_handler(self, event):
        self.previous_stage()
        
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
            if project_name == self.current_project_name:
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
            if project_name == self.current_project_name:
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

    # Select next stage.

    def next_stage(self):
        if self.current_project_def != None:

            # Cycle through stages.

            found = False
            for stage in self.current_project_def.stages:
                if found:
                    self.choose_stage(stage.name)
                    return
                if stage.name == self.current_stage_name:
                    found = True

            # Choose first stage if we fell out of the loop.

            self.choose_stage(self.current_project_def.stages[0].name)

    # Select previous stage.

    def previous_stage(self):
        if self.current_project_def != None:

            # Cycle through stages.

            previous_stage_name = self.current_project_def.stages[-1].name
            for stage in self.current_project_def.stages:
                if stage.name == self.current_stage_name:
                    self.choose_stage(previous_stage_name)
                    return
                previous_stage_name = stage.name

            # Choose last stage if we fell out of the loop.

            self.choose_stage(self.current_project_def.stages[-1].name)

    # Check action.

    def check(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.docheck(self.current_project_def, self.current_stage_def, ana=False)
        except:
            e = sys.exc_info()
            tkMessageBox.showerror('', e[1])
        self.project_view.update_status()

    # Checkana action.

    def checkana(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.docheck(self.current_project_def, self.current_stage_def, ana=True)
        except:
            e = sys.exc_info()
            tkMessageBox.showerror('', e[1])
        self.project_view.update_status()

    # Fetchlog action.

    def fetchlog(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.dofetchlog(self.current_stage_def)
        except:
            e = sys.exc_info()
            tkMessageBox.showerror('', e[1])

    # Clean action.

    def clean(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return
        try:
            project.doclean(self.current_project_def, self.current_stage_name)
        except:
            e = sys.exc_info()
            tkMessageBox.showerror('', e[1])
        self.project_view.update_status()

    # Submit action.

    def submit(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return

        try:
            project.dosubmit(self.current_project_def, self.current_stage_def, makeup=False)
        except:
            e = sys.exc_info()
            tkMessageBox.showerror('', e[1])
        BatchStatus.update_jobs()
        self.project_view.update_status()

    # Makeup action.

    def makeup(self):
        if self.current_project_def == None:
            tkMessageBox.showwarning('', 'No project selected.')
            return
        if self.current_stage_def == None:
            tkMessageBox.showwarning('', 'No stage selected.')
            return

        try:
            project.dosubmit(self.current_project_def, self.current_stage_def, makeup=True)
        except:
            e = sys.exc_info()
            tkMessageBox.showerror('', e[1])
        BatchStatus.update_jobs()
        self.project_view.update_status()

    # Update jobs action.

    def update_jobs(self):
        self.project_view.update_jobs()

    # Close window.

    def close(self):
        self.root.destroy()
