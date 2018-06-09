Attribute VB_Name = "a2ShowFriends"
Sub m2ShowFriends()

    Application.DisplayAlerts = False

    Dim sMac As String, sOut As String, sStr() As String, sDir As String, sInp As String
    Dim iLst As Integer, iCad As Integer, iRnd As Integer, iCnd As Integer, iCol As Integer, iRow As Integer, iRef As Integer
    Dim iVal As Double
    Dim oWsh As Worksheet
    Dim oRng As Range
    
    sMac = ActiveWorkbook.Name
    sOut = "TrendFriends.xlsm"
    sStr = Split("_ _ A P1 P2 P3 P4 P.1 P.2 P.3 P.4 E1 E2 E.1 E.2 S1 S2 S.1 S.2 L", " ")
    sDir = fGetDir("Select the folder containing csv files to be analyzed.") & "\"
    
    Workbooks.Add
    ActiveWorkbook.SaveAs sDir & sOut, 52
    If Sheets.Count < 3 Then Sheets.Add
    If Sheets.Count < 3 Then Sheets.Add
    If Sheets.Count < 3 Then Sheets.Add
    
    Workbooks(sMac).Sheets("Map").Copy Before:=Workbooks(sOut).Sheets(1)
    fTransfer sMac, sOut, "a4Interact"
    
    Workbooks(sOut).Activate
    Sheets(2).Name = "Friends"
    Sheets(3).Name = "Covars"
    Sheets(4).Name = "Trends"
    
    iLst = 0
    iCad = 19
    
    sInp = Dir(sDir & "*.*")
    Do While sInp <> ""
    If (Right(sInp, 4) = ".csv" Or Right(sInp, 4) = ".xls" Or Right(sInp, 5) = ".xlsx" Or Right(sInp, 5) = ".xlsm") And sInp <> sMac And Left(sInp, 5) <> "Trend" Then
    
        Workbooks.Open sDir & sInp
        For Each oWsh In ActiveWorkbook.Worksheets
        
            iLst = iLst + 1
            iRnd = Range("A1").SpecialCells(xlCellTypeLastCell).Row
            iCnd = Range("A1").SpecialCells(xlCellTypeLastCell).Column
            
            Range("A1:A" & iRnd).Copy
            Cells(1, iCnd + 1).PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
            
            Cells(1, iCnd + 2) = iLst & "_A"
            RngFun Range(Cells(2, iCnd + 2), Cells(iRnd, iCnd + 2)), "AVERAGE(RC2:RC" & iCnd & ")"
            
            For iCol = 3 To iCad
            
                Cells(1, iCnd + iCol) = iLst & "_" & sStr(iCol)
                
                RngFun Range(Cells(iRnd + 1, 3), Cells(iRnd + 1, iCnd)), sMac & "!fSetFun(R1C," & iCol & ") - " & sMac & "!fSetFun(R1C[-1]," & iCol & ")"
                RngFun Range(Cells(iRnd + 2, 3), Cells(2 * iRnd, iCnd)), "(R[-" & iRnd & "]C - R[-" & iRnd & "]C[-1]) / ((R[-" & iRnd & "]C" & iCnd + 2 & ")*(R" & iRnd + 1 & "C))"
                
                RngFun Range(Cells(2, iCnd + iCol + 1), Cells(iRnd, iCnd + iCol + 1)), "ROUND(AVERAGE(R[" & iRnd & "]C3:R[" & iRnd & "]C" & iCnd & "),4)"
                With Range(Cells(2, iCnd + iCol + 1), Cells(iRnd, iCnd + iCol + 1))
                    .Replace What:="#DIV/0!", Replacement:="", LookAt:=xlPart, SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:=False, ReplaceFormat:=False
                    .Replace What:="#VALUE!", Replacement:="", LookAt:=xlPart, SearchOrder:=xlByRows, MatchCase:=False, SearchFormat:=False, ReplaceFormat:=False
                End With
                
                If WorksheetFunction.Max(Range(Cells(2, iCnd + iCol + 1), Cells(iRnd, iCnd + iCol + 1))) > WorksheetFunction.Min(Range(Cells(2, iCnd + iCol + 1), Cells(iRnd, iCnd + iCol + 1))) Then
                    RngFun Range(Cells(2, iCnd + iCol), Cells(iRnd, iCnd + iCol)), "ROUND(RC[1]/" & WorksheetFunction.Average(Range(Cells(2, iCnd + iCol + 1), Cells(iRnd, iCnd + iCol + 1))) & ",4)"
                End If
                
                Range(Cells(2, iCnd + iCol + 1), Cells(iRnd, iCnd + iCol + 1)).Delete
                
            Next
            
            Range(Cells(1, iCnd + 1), Cells(iRnd, iCnd + iCad)).Copy
            Windows(sOut).Activate
            Sheets("Trends").Select
            
            If iLst = 1 Then
                Cells(1, 1).PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
            Else
                Cells(1, iCad * iLst - iLst + 2).PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
                Range(Cells(1, iCad * iLst - iLst + 3), Cells(1, iCad * iLst + iCad - iLst - 1)).Copy
                Cells(1, iCad * iLst - iCad - iLst + 3).PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
                For iRow = 2 To iRnd
                    iRef = WorksheetFunction.Match(Cells(iRow, 1), Range(Cells(1, iCad * iLst - iLst + 2), Cells(iRnd, iCad * iLst - iLst + 2)), 0)
                    Range(Cells(iRef, iCad * iLst - iLst + 3), Cells(iRef, iCad * iLst + iCad - iLst - 1)).Copy
                    Cells(iRow, iCad * iLst - iCad - iLst + 3).PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
                Next
                Range(Cells(1, iCad * iLst - iLst + 2), Cells(1, iCad * iLst + iCad - iLst - 1)).EntireColumn.Delete
            End If
            ActiveWorkbook.Save
            iLst = iLst + 1
            
        Next
        
        Workbooks(sInp).Close False
        
    End If
    sInp = Dir()
    Loop
    
    Workbooks(sOut).Activate
    
    Sheets("Trends").Select
    
    iRnd = Range("A1").SpecialCells(xlCellTypeLastCell).Row
    iCnd = Range("A1").SpecialCells(xlCellTypeLastCell).Column
    iLst = iCnd
    For iCol = iLst To 2
        If IsNumeric(Cells(2, iCol)) = False Or Cells(2, iCol) & "x" = "x" Then
            Cells(2, iCol).EntireColumn.Delete
            iCnd = iCnd - 1
        End If
    Next
    fFormat 0
    Range("A1:A" & iRnd).Copy
    
    Sheets("Covars").Select
    
    Range("A1").PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
    Range("A1").PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=True
    
    For iRow = 2 To iRnd
    For iCol = 2 To iRnd
        iVal = 0
        For iRef = 2 To iCnd
            iVal = iVal + (Sheets("Trends").Cells(iRow, iRef) - Sheets("Trends").Cells(iCol, iRef)) ^ 2
        Next
        Sheets("Covars").Cells(iRow, iCol) = iVal
    Next
    Next
    fFormat 0
    Range(Cells(1, 1), Cells(iRnd, iRnd)).Copy
    
    Sheets("Friends").Select
    Range("A1").PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
    
    For iRow = 2 To iRnd
    
        Range(Cells(1, 2), Cells(1, iRnd)).Copy
        Cells(iRow, iRnd + 1).PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
        Application.CutCopyMode = False
        
        Range(Cells(iRow, 2), Cells(iRow, iRnd)).Copy
        Cells(iRow + 1, iRnd + 1).PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
        Application.CutCopyMode = False
        
        ActiveSheet.Sort.SortFields.Clear
        ActiveSheet.Sort.SortFields.Add Key:=Range(Cells(iRow + 1, iRnd + 1), Cells(iRow + 1, 2 * iRnd - 1)), SortOn:=xlSortOnValues, Order:=xlAscending, DataOption:=xlSortNormal
        With ActiveSheet.Sort
            .SetRange Range(Cells(iRow, iRnd + 1), Cells(iRow + 1, 2 * iRnd - 1))
            .Header = xlYes
            .MatchCase = False
            .Orientation = xlLeftToRight
            .SortMethod = xlPinYin
            .Apply
        End With
        
    Next
    
    Range(Cells(1, 2), Cells(1, iRnd)).EntireColumn.Delete
    Range("B1") = "Self"
    Range("C1") = "Friend1"
    Range("D1") = "Friend2"
    fFormat 0
    
    Sheets("Map").Select
    ActiveWorkbook.Save
    
    MsgBox "Click on a state in Map tab to see its friend." & vbNewLine & vbNewLine & "Trends, Covars, and Friends tabs have results of analysis.", vbOKOnly, "Done"
    Application.DisplayAlerts = True
    
End Sub
