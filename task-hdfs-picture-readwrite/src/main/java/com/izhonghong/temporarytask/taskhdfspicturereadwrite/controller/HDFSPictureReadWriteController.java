package com.izhonghong.temporarytask.taskhdfspicturereadwrite.controller;

import com.izhonghong.temporarytask.taskhdfspicturereadwrite.service.PictureReadWriteHDFS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HDFSPictureReadWriteController {
    @Autowired
    PictureReadWriteHDFS pictureReadWriteHDFS;

    @PostMapping("/hdfs/picture/writing")
    public String pictureWrite(@RequestParam("BASE64Str") String base64Str,
                             @RequestParam("fileName" )String fileName){
        return pictureReadWriteHDFS.pictureWriteHDFS(base64Str, fileName);
    }

    @GetMapping("/hdfs/picture/reading")
    public String pictureRead(@RequestParam("fileName") String fileName){
        return pictureReadWriteHDFS.pictureReadHDFS(fileName);
    }
}
