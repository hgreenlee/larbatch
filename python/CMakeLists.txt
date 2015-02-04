# All of the .py files in this directory are python modules.
# We distinguish executable and non-executable .py files.
# The former are installed with execute permission.
# The latter are installed without execute permission.

# Executable python files (will also be on PYTYHONPATH).

LIST(APPEND exes root_metadata.py )

# Non-executable python modules that we want to be on PYTHONPATH

LIST(APPEND nonexes project_utilities.py xmlerror.py projectdef.py stagedef.py projectstatus.py stagestatus.py batchstatus.py )

# GUI modules.  These are not directly on PYTHONPATH, but can be imported from
# subdirectory projectgui.

LIST(APPEND guis __init__.py projectapp.py projectview.py statusview.py textwindow.py )

message(STATUS "Executable python modules ${exes}")
message(STATUS "Non-executable python modules ${nonexes}")
message(STATUS "GUI python modules ${guis}")

install(PROGRAMS ${exes} DESTINATION ${product}/${version}/python)
install(FILES ${nonexes} DESTINATION ${product}/${version}/python)
install(FILES ${guis} DESTINATION ${product}/${version}/python/project_gui_modules)
install_scripts(LIST ${exes})
