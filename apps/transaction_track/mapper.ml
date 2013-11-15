let pack_data : int*int->string = Util.marshal in
let (blockID, value) = Program.get_input() in
let blockData = Util.split_words value in
let numTrans = int_of_string (List.hd blockData) in
let rec inCoins curr count accum =
    if count <= 0 then (accum, curr)
    else
        inCoins (List.tl curr) (count - 1)
         (((List.hd curr), pack_data (int_of_string blockID,0) )::accum) in
let rec outCoins curr count accum =
    if count <= 0 then (accum,curr)
    else
        outCoins (List.tl (List.tl curr)) (count - 1) (((List.hd curr), pack_data (int_of_string blockID, int_of_string (List.hd (List.tl curr)))  )::accum) in
let rec doTrans curr count accum =
    if count <= 0 then accum
    else (
        let inCount = int_of_string (List.hd curr) in
        let outCount = int_of_string (List.hd (List.tl curr)) in
        let inTup = inCoins (List.tl (List.tl curr)) inCount accum in
        let outTup = outCoins (snd inTup) outCount (fst inTup) in
        doTrans (snd outTup) (count - 1) (fst outTup)
    )
in 
Program.set_output (List.rev (doTrans (List.tl blockData) numTrans []))