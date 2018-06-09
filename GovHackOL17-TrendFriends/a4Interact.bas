Attribute VB_Name = "a4Interact"
Private Sub Alabama_Click()
    InterX "Alabama"
End Sub
Private Sub Alaska_Click()
    InterX "Alaska"
End Sub
Private Sub Arizona_Click()
    InterX "Arizona"
End Sub
Private Sub Arkansas_Click()
    InterX "Arkansas"
End Sub
Private Sub California_Click()
    InterX "California"
End Sub
Private Sub Colorado_Click()
    InterX "Colorado"
End Sub
Private Sub Connecticut_Click()
    InterX "Connecticut"
End Sub
Private Sub Delaware_Click()
    InterX "Delaware"
End Sub
Private Sub DistofColumbia_Click()
    InterX "DistofColumbia"
End Sub
Private Sub Florida_Click()
    InterX "Florida"
End Sub
Private Sub Georgia_Click()
    InterX "Georgia"
End Sub
Private Sub Hawaii_Click()
    InterX "Hawaii"
End Sub
Private Sub Idaho_Click()
    InterX "Idaho"
End Sub
Private Sub Illinois_Click()
    InterX "Illinois"
End Sub
Private Sub Indiana_Click()
    InterX "Indiana"
End Sub
Private Sub Iowa_Click()
    InterX "Iowa"
End Sub
Private Sub Kansas_Click()
    InterX "Kansas"
End Sub
Private Sub Kentucky_Click()
    InterX "Kentucky"
End Sub
Private Sub Louisiana_Click()
    InterX "Louisiana"
End Sub
Private Sub Maine_Click()
    InterX "Maine"
End Sub
Private Sub Maryland_Click()
    InterX "Maryland"
End Sub
Private Sub Massachusetts_Click()
    InterX "Massachusetts"
End Sub
Private Sub Michigan_Click()
    InterX "Michigan"
End Sub
Private Sub Minnesota_Click()
    InterX "Minnesota"
End Sub
Private Sub Mississippi_Click()
    InterX "Mississippi"
End Sub
Private Sub Missouri_Click()
    InterX "Missouri"
End Sub
Private Sub Montana_Click()
    InterX "Montana"
End Sub
Private Sub Nebraska_Click()
    InterX "Nebraska"
End Sub
Private Sub Nevada_Click()
    InterX "Nevada"
End Sub
Private Sub NewHampshire_Click()
    InterX "NewHampshire"
End Sub
Private Sub NewJersey_Click()
    InterX "NewJersey"
End Sub
Private Sub NewMexico_Click()
    InterX "NewMexico"
End Sub
Private Sub NewYork_Click()
    InterX "NewYork"
End Sub
Private Sub NorthCarolina_Click()
    InterX "NorthCarolina"
End Sub
Private Sub NorthDakota_Click()
    InterX "NorthDakota"
End Sub
Private Sub Ohio_Click()
    InterX "Ohio"
End Sub
Private Sub Oklahoma_Click()
    InterX "Oklahoma"
End Sub
Private Sub Oregon_Click()
    InterX "Oregon"
End Sub
Private Sub Pennsylvania_Click()
    InterX "Pennsylvania"
End Sub
Private Sub RhodeIsland_Click()
    InterX "RhodeIsland"
End Sub
Private Sub SouthCarolina_Click()
    InterX "SouthCarolina"
End Sub
Private Sub SouthDakota_Click()
    InterX "SouthDakota"
End Sub
Private Sub Tennessee_Click()
    InterX "Tennessee"
End Sub
Private Sub Texas_Click()
    InterX "Texas"
End Sub
Private Sub Utah_Click()
    InterX "Utah"
End Sub
Private Sub Vermont_Click()
    InterX "Vermont"
End Sub
Private Sub Virginia_Click()
    InterX "Virginia"
End Sub
Private Sub Washington_Click()
    InterX "Washington"
End Sub
Private Sub WestVirginia_Click()
    InterX "WestVirginia"
End Sub
Private Sub Wisconsin_Click()
    InterX "Wisconsin"
End Sub
Private Sub Wyoming_Click()
    InterX "Wyoming"
End Sub

Private Sub InterX(sState As String)

    On Error GoTo xExit
    Dim iRnd As Integer
    iRnd = Sheets("Friends").Range("A1").SpecialCells(xlCellTypeLastCell).Row
    
    Dim iRow As Integer
    Dim sChk(1) As String
    
    Sheets("Map").Select
    
    For iRow = 2 To iRnd
        On Error GoTo xJump1
        sChk(0) = Replace(Sheets("Friends").Range("A" & iRow), " ", "")
        ActiveSheet.Shapes.Range(Array(sChk(0))).Select
        With Selection.ShapeRange.Fill
            .Visible = msoTrue
            .ForeColor.ObjectThemeColor = msoThemeColorAccent1
            .ForeColor.TintAndShade = 0
            .ForeColor.Brightness = 0.4
            .Transparency = 0
            .Solid
        End With
xJump1:
    Next
    
    sChk(0) = sState
    On Error GoTo xExit
    iRow = Sheets("Friends").Columns("A:A").Find(What:=sChk(0), After:=Sheets("Friends").Range("A1"), LookIn:=xlFormulas, LookAt:=xlPart, SearchOrder:=xlByRows, SearchDirection:=xlNext, MatchCase:=False, SearchFormat:=False).Row
    sChk(1) = Replace(Sheets("Friends").Range("C" & iRow), " ", "")
    
    For iRow = 1 To 0 Step -1
        ActiveSheet.Shapes.Range(Array(sChk(iRow))).Select
        With Selection.ShapeRange.Fill
            .Visible = msoTrue
            .ForeColor.RGB = RGB(192, 0, 0)
            .Transparency = 0
            .Solid
        End With
    Next
    
xExit::
End Sub
