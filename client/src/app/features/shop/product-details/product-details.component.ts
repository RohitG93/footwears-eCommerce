import { ChangeDetectorRef, Component, inject, OnInit } from '@angular/core';
import { Product } from '../../../shared/Models/product';
import { ShopService } from '../../../core/services/shop.service';
import { ActivatedRoute } from '@angular/router';
import { CurrencyPipe } from '@angular/common';
import { MatIcon } from '@angular/material/icon';
import { MatButton } from '@angular/material/button';
import { MatFormField, MatInput, MatLabel } from '@angular/material/input';
import { MatDivider } from '@angular/material/list';
  
@Component({
  selector: 'app-product-details',
  imports: [
    CurrencyPipe,
    MatButton,
    MatIcon,
    MatFormField,
    MatInput,
    MatLabel,
    MatDivider,
  ],
  templateUrl: './product-details.component.html',
  styleUrl: './product-details.component.scss',
})
export class ProductDetailsComponent implements OnInit {

  private shopservice = inject(ShopService);
  private activatedRoute = inject(ActivatedRoute);
  product: Product | null = null;
  private cdr = inject(ChangeDetectorRef);

  ngOnInit(): void {
    this.loadProduct();
  }

  loadProduct() {
    const id = this.activatedRoute.snapshot.paramMap.get('id');
    if (id) {
      this.shopservice.getProductById(+id).subscribe({
        next: (product) => {
          this.product = product;
          this.cdr.detectChanges();
        },
        error: (error) => {
          console.error('Error fetching product details:', error);
        }
      });
    }
  }
}
