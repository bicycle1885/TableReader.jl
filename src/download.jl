# The following function is taken from the standard library of Julia
# (base/download.jl, commit: 05574672881f341104a30519ef9d7534540e175e).
# License is MIT: https://julialang.org/license
function find_curl()
    if Sys.isapple() && Sys.isexecutable("/usr/bin/curl")
        "/usr/bin/curl"
    elseif Sys.iswindows() && Sys.isexecutable(joinpath(get(ENV, "SYSTEMROOT", "C:\\Windows"), "System32\\curl.exe"))
        joinpath(get(ENV, "SYSTEMROOT", "C:\\Windows"), "System32\\curl.exe")
    elseif Sys.which("curl") !== nothing
        "curl"
    else
        nothing
    end
end
