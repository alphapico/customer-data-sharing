using CustomerDataSharingLogic.ExternalConnections.TMDB;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace CustomerDataSharingLogic.IMSE5515
{
    public static class IMSE5515CsvHelper
    {
        const string header = @",,,,,,,Parameter grouping,CX,CX,CX,CX,CX,CX,CY,CY,CY,CY,CY,CY,IV,IV,IV,IV,IV,IV,IV adj,IV adj,IV adj,IV adj,IV adj,IV adj,UF,UF,UF,UF,UF,UF,Ldom,Ldom,Ldom,Ldom,Ldom,Ldom,B.X,B.X,B.Z,B.Z,G.X,G.X,G.Z,G.Z,R.X,R.X,R.Z,R.Z,B.X.a,B.X.b,B.Y.a,B.Y.b,B.Z.a,B.Z.b,B.T.a,B.T.b,G.X.a,G.X.b,G.Y.a,G.Y.b,G.Z.a,G.Z.b,G.T.a,G.T.b,R.X.a,R.X.b,R.Y.a,R.Y.b,R.Z.a,R.Z.b,R.T.a,R.T.b,B.X.a.u,B.X.a.v,B.X.a.w,B.X.b.u,B.X.b.v,B.X.b.w,B.Y.a.u,B.Y.a.v,B.Y.a.w,B.Y.b.u,B.Y.b.v,B.Y.b.w,B.Z.a.u,B.Z.a.v,B.Z.a.w,B.Z.b.u,B.Z.b.v,B.Z.b.w,B.T.a.u,B.T.a.v,B.T.a.w,B.T.b.u,B.T.b.v,B.T.b.w,G.X.a.u,G.X.a.v,G.X.a.w,G.X.b.u,G.X.b.v,G.X.b.w,G.Y.a.u,G.Y.a.v,G.Y.a.w,G.Y.b.u,G.Y.b.v,G.Y.b.w,G.Z.a.u,G.Z.a.v,G.Z.a.w,G.Z.b.u,G.Z.b.v,G.Z.b.w,G.T.a.u,G.T.a.v,G.T.a.w,G.T.b.u,G.T.b.v,G.T.b.w,R.X.a.u,R.X.a.v,R.X.a.w,R.X.b.u,R.X.b.v,R.X.b.w,R.Y.a.u,R.Y.a.v,R.Y.a.w,R.Y.b.u,R.Y.b.v,R.Y.b.w,R.Z.a.u,R.Z.a.v,R.Z.a.w,R.Z.b.u,R.Z.b.v,R.Z.b.w,R.T.a.u,R.T.a.v,R.T.a.w,R.T.b.u,R.T.b.v,R.T.b.w
,,,,,,,Chip color,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,BLUE,BLUE,GREEN,GREEN,GREEN,GREEN,RED,RED,RED,RED,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,RED,RED,RED,RED,RED,RED,RED,RED,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED
Calendar Week,Pocket Position,DMC,Delivery ID,Handling Unit,Reel Label,Dry Pack Label,Product Box Label,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20,50,20,50,20,50,20,50,20,50,20,50,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20";

        public static string CreateCsvFile(REEL_DATA reelData, List<REP_D_VJ_REEL_DATALOG> data)
        {
            String content = header;

            int maxPocketPosition = data.Max(d => d.REEL_SEQ_POS);
            data = data.OrderBy(d => d.REEL_SEQ_POS).ToList();
            int currentPocketPosition = 0;
            var lineEntries = new List<object>();
            for (int i = 1; i <= maxPocketPosition; i++)
            {
                if (i == data[currentPocketPosition].REEL_SEQ_POS)
                {
                    var line = data[currentPocketPosition];
                    lineEntries = new List<object>()
                    {
                        reelData.CalendarWeek,
                        line.REEL_SEQ_POS,
                        line.DMC,
                        reelData.Delivery_ID,
                        reelData.Handling_Unit,
                        reelData.Reel_Label,
                        reelData.Dry_Pack_Label,
                        reelData.Product_Box_Label,
                        line.CX_BLUE_20,
                        line.CX_BLUE_50,
                        line.CX_GREEN_20,
                        line.CX_GREEN_50,
                        line.CX_RED_20,
                        line.CX_RED_50,
                        line.CY_BLUE_20,
                        line.CY_BLUE_50,
                        line.CY_GREEN_20,
                        line.CY_GREEN_50,
                        line.CY_RED_20,
                        line.CY_RED_50,
                        line.IV_BLUE_20,
                        line.IV_BLUE_50,
                        line.IV_GREEN_20,
                        line.IV_GREEN_50,
                        line.IV_RED_20,
                        line.IV_RED_50,
                        line.IV_BLUE_20*0.92,
                        line.IV_BLUE_50*0.92,
                        line.IV_GREEN_20*0.98,
                        line.IV_GREEN_50*0.98,
                        line.IV_RED_20*0.97,
                        line.IV_RED_50*0.97,
                        line.UF_BLUE_20,
                        line.UF_BLUE_50,
                        line.UF_GREEN_20,
                        line.UF_GREEN_50,
                        line.UF_RED_20,
                        line.UF_RED_50,
                        line.LdomBlue20,
                        line.LdomBlue50,
                        line.LdomGreen20,
                        line.LdomGreen50,
                        line.LdomRed20,
                        line.LdomRed50,

                        line.BXBlue20,
                        line.BXBlue50,
                        line.BZBlue20,
                        line.BZBlue50,
                        line.BXGreen20,
                        line.BXGreen50,
                        line.BZGreen20,
                        line.BZGreen50,
                        line.BXRed20,
                        line.BXRed50,
                        line.BZRed20,
                        line.BZRed50,

                        line.BXaBlue20,
                        line.BXbBlue20,
                        line.BYaBlue20,
                        line.BYbBlue20,
                        line.BZaBlue20,
                        line.BZbBlue20,
                        line.BTaBlue20,
                        line.BTbBlue20,
                        line.GXaGreen20,
                        line.GXbGreen20,
                        line.GYaGreen20,
                        line.GYbGreen20,
                        line.GZaGreen20,
                        line.GZbGreen20,
                        line.GTaGreen20,
                        line.GTbGreen20,
                        line.RXaRed20,
                        line.RXbRed20,
                        line.RYaRed20,
                        line.RYbRed20,
                        line.RZaRed20,
                        line.RZbRed20,
                        line.RTaRed20,
                        line.RTbRed20,

                        line.BXauBlue20,
                        line.BXavBlue20,
                        line.BXawBlue20,
                        line.BXbuBlue20,
                        line.BXbvBlue20,
                        line.BXbwBlue20,
                        line.BYauBlue20,
                        line.BYavBlue20,
                        line.BYawBlue20,
                        line.BYbuBlue20,
                        line.BYbvBlue20,
                        line.BYbwBlue20,
                        line.BZauBlue20,
                        line.BZavBlue20,
                        line.BZawBlue20,
                        line.BZbuBlue20,
                        line.BZbvBlue20,
                        line.BZbwBlue20,
                        line.BTauBlue20,
                        line.BTavBlue20,
                        line.BTawBlue20,
                        line.BTbuBlue20,
                        line.BTbvBlue20,
                        line.BTbwBlue20,

                        line.GXauGreen20,
                        line.GXavGreen20,
                        line.GXawGreen20,
                        line.GXbuGreen20,
                        line.GXbvGreen20,
                        line.GXbwGreen20,
                        line.GYauGreen20,
                        line.GYavGreen20,
                        line.GYawGreen20,
                        line.GYbuGreen20,
                        line.GYbvGreen20,
                        line.GYbwGreen20,
                        line.GZauGreen20,
                        line.GZavGreen20,
                        line.GZawGreen20,
                        line.GZbuGreen20,
                        line.GZbvGreen20,
                        line.GZbwGreen20,
                        line.GTauGreen20,
                        line.GTavGreen20,
                        line.GTawGreen20,
                        line.GTbuGreen20,
                        line.GTbvGreen20,
                        line.GTbwGreen20,

                        line.RXauRed20,
                        line.RXavRed20,
                        line.RXawRed20,
                        line.RXbuRed20,
                        line.RXbvRed20,
                        line.RXbwRed20,
                        line.RYauRed20,
                        line.RYavRed20,
                        line.RYawRed20,
                        line.RYbuRed20,
                        line.RYbvRed20,
                        line.RYbwRed20,
                        line.RZauRed20,
                        line.RZavRed20,
                        line.RZawRed20,
                        line.RZbuRed20,
                        line.RZbvRed20,
                        line.RZbwRed20,
                        line.RTauRed20,
                        line.RTavRed20,
                        line.RTawRed20,
                        line.RTbuRed20,
                        line.RTbvRed20,
                        line.RTbwRed20
                    };
                    currentPocketPosition++;
                }
                else
                {
                    lineEntries = new List<object>()
                    {
                        reelData.CalendarWeek,
                        i
                    };
                }

                content += Environment.NewLine + String.Join(",", lineEntries.Select(l => l is double ? ((double)l).ToString("0.0000", CultureInfo.InvariantCulture) : l.ToString()));
            }

            return content;
        }

        const string oldHeader = @",,,,,,,Parameter grouping,CX,CX,CX,CX,CX,CX,CY,CY,CY,CY,CY,CY,IV,IV,IV,IV,IV,IV,UF,UF,UF,UF,UF,UF
,,,,,,,Chip color,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED
Calendar Week,Pocket Position,DMC,Delivery ID,Handling Unit,Reel Label,Dry Pack Label,Product Box Label,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.";

        public static string CreateOldCsvFile(REEL_DATA reelData, List<REP_D_VJ_REEL_DATALOG> data)
        {
            String content = oldHeader;

            int maxPocketPosition = data.Max(d => d.REEL_SEQ_POS);
            data = data.OrderBy(d => d.REEL_SEQ_POS).ToList();
            int currentPocketPosition = 0;
            var lineEntries = new List<object>();
            for (int i = 1; i <= maxPocketPosition; i++)
            {
                if (i == data[currentPocketPosition].REEL_SEQ_POS)
                {
                    var line = data[currentPocketPosition];
                    lineEntries = new List<object>()
                    {
                        reelData.CalendarWeek,
                        line.REEL_SEQ_POS,
                        line.DMC,
                        reelData.Delivery_ID,
                        reelData.Handling_Unit,
                        reelData.Reel_Label,
                        reelData.Dry_Pack_Label,
                        reelData.Product_Box_Label,
                        line.CX_BLUE_20,
                        line.CX_BLUE_50,
                        line.CX_GREEN_20,
                        line.CX_GREEN_50,
                        line.CX_RED_20,
                        line.CX_RED_50,
                        line.CY_BLUE_20,
                        line.CY_BLUE_50,
                        line.CY_GREEN_20,
                        line.CY_GREEN_50,
                        line.CY_RED_20,
                        line.CY_RED_50,
                        line.IV_BLUE_20,
                        line.IV_BLUE_50,
                        line.IV_GREEN_20,
                        line.IV_GREEN_50,
                        line.IV_RED_20,
                        line.IV_RED_50,
                        line.UF_BLUE_20,
                        line.UF_BLUE_50,
                        line.UF_GREEN_20,
                        line.UF_GREEN_50,
                        line.UF_RED_20,
                        line.UF_RED_50
                    };
                    currentPocketPosition++;
                }
                else
                {
                    lineEntries = new List<object>()
                    {
                        reelData.CalendarWeek,
                        i
                    };
                }

                content += Environment.NewLine + String.Join(",", lineEntries.Select(l => l is double ? ((double)l).ToString("0.0000", CultureInfo.InvariantCulture) : l.ToString()));
            }

            return content;
        }
    }


    public class REEL_DATA
    {
        public String CalendarWeek { get; set; }
        public String Delivery_ID { get; set; }
        public String Handling_Unit { get; set; }
        public String Reel_Label { get; set; }
        public String Dry_Pack_Label { get; set; }
        public String Product_Box_Label { get; set; }
    }
}