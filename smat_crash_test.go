//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// +build gofuzz

package moss

import (
	"log"
	"testing"

	"github.com/mschoch/smat"
)

// Crashers found by smat, captured as pairs of strings.  A pair is a
// short descrption of the crash then the corresponding crash-input.
var smatCrashers = []string{
	"panic: get mismatch, got: :990, mirror: :990:990",
	"annot t ue otane fo " +
		"  _tJ______  _HR_4fX" +
		"_0U26_22_E7D_00kT3G1" +
		"6cqP4p5_02o_OtYa3QY " +
		"5____5_____ ___ t  i" +
		"misng v J  _1p_W_TT_" +
		"YZ25SX0_D_k_fIT__AKX" +
		"2_5_0X_6\t\t\t\t\t\t\t\t\t_62" +
		"_F1Xp_62Q63C_o_f__Ez" +
		"6Q_C5Qt_2_o0_0B __Y3" +
		"0U_Z0I20_O_BoEK__eZ_" +
		"0_ tuE__Z0G6___26___" +
		"_pvIG_om _d0_fd_1o  " +
		"%   doomipo my",

	"panic: get mismatch, got: , mirror: :1000",
	"cvZ8Vn6Iu_7_7F_m_h__" +
		"xT e_c_Pa7spjT5B2_3_" +
		"g_sR2Xf6AJq5_F1___U2" +
		"6_5322\x9b2\x9b2\x9b",

	"panic: get mismatch, got: :x, mirror: x:x:x:x:x:x",
	"on13  to___o____u__5" +
		"_____   _tJ______  _" +
		"HdR_V4f_0U26_22M_E7D" +
		"_00VkTL3G71cV36c7aqP" +
		"4p5_Pv7xb02o_OtIYa3Q" +
		"Y9 5____5___c  imiss" +
		"ng ve\xfdb \xfdJve  _9aL1p" +
		"_W_jTT_YzZ5S307_D_k_" +
		"fIT6_9_AKX2_5H_Y0N74" +
		"X_69b\t\t\t\t\t\t\t\t\t_6z2_F" +
		"19Xp_Y2Q63IC_oL4_f_P" +
		"_Ez634QL_C57QtP_3_o0" +
		"_0B __Y30cwUsM_zZ0IS" +
		"20__BoEK_D_eVZ3e_0_\t" +
		" \tetvaue4Er__Z0G46__" +
		"_2UH6R___4_MpvuUIG_r" +
		"om u7d0_Nfd_1oen \t\t\t" +
		"    endooanuo mahor7" +
		"mDit aciondthat15qer" +
		"r S0S_S2Ro3\x12",

	"panic: iterMirror val != iter val",
	"_rj_h7tuxX_f8P6160f7" +
		"wP19_8nLt_a2QEyY3__J" +
		"393Emt_EHbR7w0_rinvo" +
		"k\xa1ng setup action-6",

	"fatal error: all goroutines are asleep - deadlock!",
	"\xd4\x00\u007f\x06\xef\xd4D\xd49",

	"panic: open /.../mossStoreSMAT673923094: too many open files",
	"\xacY\x00\x00\x15\xc4\x15\x80@\x00\x00\x00\x00\x01\x00\x00\x01\x00",

	"panic: iterMirror val != iter val",
	" ee\xbd|\x03\x80\x0e\xc1\x80\xbf|06\xbc|\x91\x9b?\xc4" +
		"[\x99\xb4Ģ\xce=",
}

// Test the previous crashers found by smat.
func TestSmatCrashers(t *testing.T) {
	smatDebugPrev := smatDebug
	smatDebug = false // Use true when diagnosing a crash.

	// smat.Logger = log.New(os.Stderr, "smat ", log.LstdFlags)

	for i := 0; i < len(smatCrashers); i += 2 {
		desc := smatCrashers[i]
		crasher := []byte(smatCrashers[i+1])

		log.Printf("testing smat crasher: (%d) %s", i/2, desc)

		// fuzz the crasher input
		smat.Fuzz(&smatContext{}, smat.ActionID('S'), smat.ActionID('T'),
			actionMap, crasher)
	}

	smatDebug = smatDebugPrev
}
