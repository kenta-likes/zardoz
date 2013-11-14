open Util;;

let main (args : string array) : unit =
  if Array.length args < 3 then
    print_endline "Usage: grades <filename>"
  else
    let filename = args.(2) in
    let students : student list = load_grades filename in
    let kv_pairs = 
      let f d = 
        string_of_int d.id_num, d.course_grades in
      List.rev_map f students in
    let reduced = 
      Map_reduce.map_reduce "grades" "mapper" "reducer" kv_pairs in
    print_reduced_courses reduced 

in

main Sys.argv
