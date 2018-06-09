Attribute VB_Name = "a1ShowTrends"
Sub m1ShowTrends()

    Application.DisplayAlerts = False

    Dim sMac As String, sOut As String, sDir As String, sInp As String, sShp(0) As String
    Dim iRnd As Integer, iCnd As Integer, iCol As Integer, iRow As Integer
    
    sMac = ActiveWorkbook.Name
    sOut = "TrendMap.xlsm"
    fGetFile 0
    sDir = ActiveWorkbook.Path & "\"
    sInp = ActiveWorkbook.Name
    
    Workbooks.Add
    ActiveWorkbook.SaveAs sDir & sOut, 52
    
    Workbooks(sMac).Sheets("Map").Copy Before:=Workbooks(sOut).Sheets(1)
    fTransfer sMac, sOut, "a4Interact"
    
    Workbooks(sOut).Activate
    Sheets(2).Name = "Data"
    
    Workbooks(sInp).Activate
    Sheets(1).Select
    
    iRnd = Range("A1").SpecialCells(xlCellTypeLastCell).Row
    iCnd = Range("A1").SpecialCells(xlCellTypeLastCell).Column
    
    Range(Cells(1, 1), Cells(iRnd, iCnd)).Copy
    
    Workbooks(sOut).Activate
    Sheets("Data").Select
    Range("A1").PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
    Cells(1, iCnd + 1) = "State"
    Cells(1, iCnd + 2) = "Min"
    Cells(1, iCnd + 3) = "Max"
    RngFun Range(Cells(2, iCnd + 1), Cells(iRnd, iCnd + 1)), "SUBSTITUTE(RC1,"" "","""")"
    RngFun Range(Cells(2, iCnd + 2), Cells(iRnd, iCnd + 2)), "MIN(RC2:RC[-1])"
    RngFun Range(Cells(2, iCnd + 3), Cells(iRnd, iCnd + 3)), "MAX(RC2:RC[-1])"
    Workbooks(sInp).Close False
    
    Workbooks(sOut).Activate
    Sheets("Map").Select
    
    For iCol = 2 To iCnd Step 3
    For iRow = 2 To iRnd
        On Error GoTo xJump1
        sShp(0) = Sheets("Data").Cells(iRow, iCnd + 1)
        Sheets("Map").Shapes.Range(Array(sShp(0))).Select
        With Selection.ShapeRange.Fill
            .Visible = msoTrue
            .ForeColor.ObjectThemeColor = msoThemeColorAccent4
            .ForeColor.TintAndShade = 0
            .ForeColor.Brightness = Round((Sheets("Data").Cells(iRow, iCol) - Sheets("Data").Cells(iRow, iCnd + 2)) / Sheets("Data").Cells(iRow, iCnd + 3), 2)
            .Transparency = 0
            .Solid
        End With
        Sheets("Map").Range("A1").Select
xJump1:
    Next
    Application.Wait Now + #12:00:01 AM#
    Next
    
    MsgBox "Done"
    Application.DisplayAlerts = True

End Sub

