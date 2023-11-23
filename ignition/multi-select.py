"""
This module provides function scripts for extension functions and a property change event handler for selecting multiple cells in an Ignition Power Table column for bulk value updates.

Functions:
- onMouseClick(self, rowIndex, colIndex, colName, value, event):
    Triggered when a cell in the table is clicked. Handles various click behaviors including Ctrl-click for individual cell selection and Shift-click for range selection. Maintains a list of selected cells for potential batch updates.

- onCellEdited(self, rowIndex, colIndex, colName, oldValue, newValue):
    Called post cell modification. If certain cells (like ETO, Notes, Scope) are altered, it replicates this change to all other selected cells in the list. Helps in applying bulk changes across selected cells.

- configureCell(self, value, textValue, selected, rowIndex, colIndex, colName, rowView, colView):
    Allows the customization of each cell's appearance based on conditions. For example, cells could have different background colors based on their content or status. Additionally, it highlights the cells that are in the selection list.

- configureEditor(self, colIndex, colName):
    Determines the type of editor used for specific columns. For instance, for the 'Scope' column, a dropdown list of options is displayed to choose from.

- initialize(self):
    Executed when the window containing the table opens or when the template that houses it loads. Provides an opportunity for further initializing the table, such as specifying a particular row selection or performing an initial sort.

"""
__author__ = "Tyson Trail"
__version__ = "1.0.0"
__maintainer__ = "Tyson Trail"


def onMouseClick(self, rowIndex, colIndex, colName, value, event):
    """
    Called when the user clicks on a table cell.

    Arguments:
            self: A reference to the component that is invoking this function.
            rowIndex: Index of the row, starting at 0, relative to the underlying
                      dataset
            colIndex: Index of the column starting at 0, relative to the
                      underlying dataset
            colName: Name of the column in the underlying dataset
            value: The value at the location clicked on
            event: The MouseEvent object that caused this click event
    """
    print("Mouse clicked on row: {}, column: {}".format(rowIndex, colIndex))

    # Handle Ctrl-Click for individual cell selection
    if event.isControlDown():
        # If there's a previous selection, check if the clicked column matches the first selected column
        if self.selectedCellsList.rowCount > 0:
            firstSelectedColumn = self.selectedCellsList.getValueAt(0, "column")
            if colIndex != firstSelectedColumn:
                print("Column mismatch. Selection not updated.")
                return  # Exit early as columns don't match

        if colName in ["Scope", "ETO", "Notes"]:
            newRow = [rowIndex, colIndex]
            updatedCells = system.dataset.addRow(self.selectedCellsList, newRow)
            self.selectedCellsList = updatedCells

        # Update last clicked cell for further operations
        self.lastClickedRow = rowIndex
        self.lastClickedCol = colIndex

        # Handle Shift-Click for range selection
    elif event.isShiftDown() and (
        self.lastClickedRow != -1 and self.lastClickedCol != -1
    ):
        if self.lastClickedCol == colIndex:
            # Create a dictionary to map "Well Name" values to model indices
            wellNameToModelIndex = {}
            for modelIndex in range(self.data.rowCount):
                wellName = self.data.getValueAt(modelIndex, "Wellsite")
                wellNameToModelIndex[wellName] = modelIndex

            # Get "Well Name" for clicked and last clicked rows
            clickedWellName = self.data.getValueAt(rowIndex, "Wellsite")
            lastClickedWellName = self.data.getValueAt(self.lastClickedRow, "Wellsite")

            # Convert model indices to view indices
            clickedViewIndex = -1
            lastClickedViewIndex = -1
            for viewIndex in range(self.viewDataset.rowCount):
                wellNameInView = self.viewDataset.getValueAt(viewIndex, "Well")
                if wellNameInView == clickedWellName:
                    clickedViewIndex = viewIndex
                if wellNameInView == lastClickedWellName:
                    lastClickedViewIndex = viewIndex

            # Calculate the range for view indices
            startViewIndex = min(clickedViewIndex, lastClickedViewIndex)
            endViewIndex = max(clickedViewIndex, lastClickedViewIndex)

            # Loop through the selected range in the view
            for viewIndex in range(startViewIndex, endViewIndex + 1):
                wellNameInView = self.viewDataset.getValueAt(viewIndex, "Well")
                modelIndex = wellNameToModelIndex.get(wellNameInView, -1)

                # If model index is valid, update the selected cells
                if modelIndex != -1:
                    newRow = [modelIndex, colIndex]
                    self.selectedCellsList = system.dataset.addRow(
                        self.selectedCellsList, newRow
                    )

    # If neither Ctrl nor Shift is pressed, clear the selection
    else:
        self.selectedCellsList = system.dataset.clearDataset(self.selectedCellsList)
        self.selectedColumn = -1

        # Update last clicked cell for further operations
        self.lastClickedRow = rowIndex
        self.lastClickedCol = colIndex

    # Repaint at the end
    self.repaint()


def onCellEdited(self, rowIndex, colIndex, colName, oldValue, newValue):
    """
    Called when the user has edited a cell in the table. It is up to the
    implementation of this function to alter the underlying data that drives
    the table. This might mean altering the dataset directly, or running a SQL
    UPDATE query to update data in a database.

    Arguments:
        self: A reference to the component that is invoking this function.
        rowIndex: Index of the row that was edited, relative to the underlying
                  dataset
        colIndex: Index of the column that was edited, relative to the
                  underlying dataset
        colName: Name of the column in the underlying dataset
        oldValue: The old value at the location, before it was edited
        newValue: The new value input by the user.
    """
    # Update the cell that was directly edited
    updatedData = system.dataset.setValue(self.data, rowIndex, colIndex, newValue)

    # Check if the edited cell is a date cell in the ETO column
    if colName in ["ETO", "Notes", "Scope"]:
        # If the edited cell is in the ETO column, loop over the selected cells and update them
        for i in range(self.selectedCellsList.rowCount):
            selectedRow = self.selectedCellsList.getValueAt(i, "row")
            selectedColumn = self.selectedCellsList.getValueAt(i, "column")

            # Ensure we only update the date column cells
            if self.data.getColumnName(selectedColumn) == colName:
                updatedData = system.dataset.setValue(
                    updatedData, selectedRow, selectedColumn, newValue
                )

    # Update the table's data with the modified dataset
    self.data = updatedData

    # This line appears to be trying to set a value in columnAttributesData.
    # This should not be needed for this functionality, but I've left it intact.
    # However, you might want to review its necessity in your application.
    self.columnAttributesData = system.dataset.setValue(
        self.columnAttributesData, rowIndex, colIndex, newValue
    )


def configureCell(
    self, value, textValue, selected, rowIndex, colIndex, colName, rowView, colView
):
    """
    Provides a chance to configure the contents of each cell. Return a
    dictionary of name-value pairs with the desired attributes. Available
    attributes include: 'background', 'border', 'font', 'foreground',
    'horizontalAlignment', 'iconPath', 'text', 'toolTipText',
    'verticalAlignment'

    You may also specify the attribute 'renderer', which is expected to be a
    javax.swing.JComponent which will be used to render the cell.

    Arguments:
        self: A reference to the component that is invoking this function.
        value: The value in the dataset at this cell
        textValue: The text the table expects to display at this cell (may be
                    overridden by including 'text' attribute in returned dictionary)
        selected: A boolean indicating whether this cell is currently selected
        rowIndex: The index of the row in the underlying dataset
        colIndex: The index of the column in the underlying dataset
        colName: The name of the column in the underlying dataset
        rowView: The index of the row, as it appears in the table view
                    (affected by sorting)
        colView: The index of the column, as it appears in the table view
                    (affected by column re-arranging and hiding)
    """
    # Default background color
    backgroundColor = system.gui.color(255, 255, 255)

    # Check your conditions
    isChecked = self.data.getValueAt(rowIndex, "Enabled")
    autoOffline = self.data.getValueAt(rowIndex, "Scope")
    ETO = self.data.getValueAt(rowIndex, "ETO")
    time = system.date.now()

    if (
        ETO == None
    ):  # populates ETO with something other than BLANK to avoid errors when running the below calcs
        ETO = time

    if autoOffline == "Auto Offline":
        backgroundColor = system.gui.color(255, 194, 194)  # Red
    elif isChecked == 1 and (
        autoOffline == "Available" or autoOffline == "Auto Available"
    ):
        backgroundColor = system.gui.color(194, 255, 194)  # Green
    elif system.date.isAfter(time, ETO):  # Flag if current date is after ETO
        backgroundColor = system.gui.color(233, 194, 255)  # Purple
    else:
        backgroundColor = system.gui.color(247, 255, 78, 135)  # Yellow

    # Extract the selectedCellsList dataset.
    selectedCells = self.selectedCellsList

    # Check if the current cell is in the selected cells dataset.
    for i in range(selectedCells.rowCount):
        if (
            selectedCells.getValueAt(i, "row") == rowIndex
            and selectedCells.getValueAt(i, "column") == colIndex
        ):
            # If the cell is in the list, change its background color.
            backgroundColor = system.gui.color(
                200, 200, 255
            )  # Example light blue color.
            break

    return {"background": backgroundColor}


def configureEditor(self, colIndex, colName):
    """
    Provides a chance to configure how each column is edited. Return a
    dictionary of name-value pairs with desired editor attributes. Visual
    attributes to modify existing editors include: 'background', 'border',
    'font', 'foreground', 'horizontalAlignment', 'toolTipText', and
    'verticalAlignment'.

    If the attribute 'options' is specified, it is expected to be a list of
    tuples, with each tuple representing (value, label). The editor in this
    case will become a dropdown list. To override the dropdown's popup row
    heights, you can add a 'rowHeight' key to the returned dictionary.

    If the attribute 'editor' is specified, it is expected to be an instance
    of javax.swing.table.TableCellEditor, and other attributes will be
    ignored.

    Arguments:
            self: A reference to the component that is invoking this function.
            colIndex: The index of the column in the underlying dataset
            colName: The name of the column in the underlying dataset
    """
    scopes = [
        ("Available", "Available"),
        ("Operations", "Operations"),
        ("Maintenance", "Maintenance"),
        ("D&C", "D&C"),
        ("Projects", "Projects"),
        ("D/S Issue-Canlin", "D/S-Canlin"),
        ("D/S Issue-3rd Party", "D/S-3rd Party"),
    ]

    if colName == "Scope":
        return {"options": scopes}


def initialize(self):
    """
    Called when the window containing this table is opened, or the template
    containing it is loaded. Provides a chance to initialize the table
    further, for example, selecting a specific row.

    Arguments:
            self: A reference to the component that is invoking this function.
    """

    def sortTable():
        self.getTable().sortColumn("Active", True, True)

    system.util.invokeLater(sortTable)


# Event handler script for the Power Table's propertyChange event
# Check if the property that changed is 'data'
if event.propertyName == "data":
    print("Data property changed detected.")

    # Access the dataset from the Power Table
    data = event.source.data

    # Check if the column exists in the dataset
    if "Flow" in data.columnNames:
        print("Column exists in dataset.")

        sumValue = 0
        # Sum the values
        for row in range(data.rowCount):
            sumValue += data.getValueAt(row, "Flow")

        # Set the value of the Numeric Label
        window = system.gui.getParentWindow(event)
        rootContainer = window.getRootContainer()
        numericLabel = rootContainer.getComponent("NumericLabel1")
        numericLabel.value = sumValue

        print("Sum value set to: %s" % sumValue)
    else:
        print("Column does not exist in dataset.")
